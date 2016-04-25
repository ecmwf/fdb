/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "eckit/thread/AutoLock.h"
#include "eckit/log/BigNum.h"
#include "eckit/log/Bytes.h"
#include "eckit/parser/JSON.h"
#include "eckit/log/Timer.h"

#include "fdb5/legacy/IndexCache.h"

using namespace eckit;

namespace fdb5 {
namespace legacy {

//----------------------------------------------------------------------------------------------------------------------

IndexCache::IndexCache(const PathName& cache):
    cache_(cache),
    files_(0),
    fields_(0),
    points_(0),
    bytes_(0)
{
    Timer timer("Loading cache");
    cache_.mkdir();
    PathName c = cache_ + "/cache";
    Log::info() << "IndexCache is " << c << std::endl;
    std::ifstream in(c.localPath());
    std::string p;
    while(in >> p) {
        eckit::PathName path(p);
        GribFileSummary s; s.load(summary(path));
        update(path,s);
    }
    Log::info() << "Files: " << BigNum(files_) << std::endl;
    Log::info() << "Fields: " << BigNum(fields_)<< std::endl;
    Log::info() << "Points: " << BigNum(points_)<< std::endl;
    Log::info() << "Bytes: " << BigNum(bytes_)<< " (" << Bytes(bytes_) << ")" << std::endl;
}

IndexCache::~IndexCache()
{

}

std::vector<PathName> IndexCache::candidates(const FieldInfoKey& k1, const FieldInfoKey& k2)
{
    AutoLock<Mutex> lock(mutex_);
    std::vector<PathName> result;

    for(std::map<PathName,GribFileSummary>::const_iterator j = ready_.begin(); j != ready_.end(); ++j)
        if((*j).second.match(k1,k2))
            result.push_back((*j).first);

    return result;
}

bool IndexCache::ready(const PathName& path)
{
    return ready_.find(path) != ready_.end();
}

void IndexCache::ready(const PathName& path, const GribFileSummary& summary)
{
    AutoLock<Mutex> lock(mutex_);

    if(ready_.find(path) == ready_.end())
    {
        PathName cache = cache_ + "/cache";
        std::ofstream out(cache.localPath(),std::ios::app);
        out << path << std::endl;

        update(path, summary);
    }
}

void IndexCache::update(const PathName& path, const GribFileSummary& s)
{
    AutoLock<Mutex> lock(mutex_);
    ready_[path] = s;

    files_++;
    fields_ += s.fields_;
    points_ += s.points_;
    bytes_ += s.bytes_;
}

void IndexCache::json(eckit::JSON& s) const
{
    s.startObject();
    s << "files" << files_;
    s << "fields" << fields_;
    s << "points" << points_;
    s << "bytes" << bytes_;
    s.endObject();
}

PathName IndexCache::btree(const PathName& path)
{
    std::string name = path.asString();
    std::replace(name.begin(), name.end(), '/', '_'); // TODO: this does not garantee unicity
    return cache_ + "/" + name + ".btree";
}

PathName IndexCache::summary(const PathName& path)
{
    std::string name = path.asString();
    std::replace(name.begin(), name.end(), '/', '_'); // TODO: this does not garantee unicity
    return cache_ + "/" + name + ".summary";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace legacy
} // namespace fdb5

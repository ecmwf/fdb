/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/Index.h"

#include "eckit/config/Resource.h"
#include "eckit/io/FileHandle.h"
#include "eckit/parser/JSON.h"
#include "eckit/parser/JSONParser.h"
#include "eckit/thread/AutoLock.h"


namespace fdb5 {

//-----------------------------------------------------------------------------

Index *Index::create(const Key &key, const eckit::PathName &path, Index::Mode mode ) {

    static std::string fdbIndexType = eckit::Resource<std::string>( "fdbIndexType", "BTreeIndex" );

    return IndexFactory::build(fdbIndexType, key, path, mode);
}

//-----------------------------------------------------------------------------

Index::Index(const Key &key, const eckit::PathName &path, Index::Mode mode ) :
    mode_(mode),
    path_(path),
    files_(),
    axes_(),
    key_(key),
    prefix_(key.valuesToString()) {

    eckit::PathName json(jsonFile());

    if ( json.exists() ) {
        eckit::Log::info() << "Load " << json << std::endl;
        std::ifstream f(json.asString().c_str());

        eckit::JSONParser parser(f);

        eckit::Value v = parser.parse();
//        eckit::Log::info() << "JSON: " << v << std::endl;

        prefix_ = v["prefix"].as<std::string>();

        files_.load(v["files"]);
//        eckit::Log::info() << "Files " << files_ << std::endl;

        axes_.load(v["axes"]);
        eckit::Log::info() << "Axis " << axes_ << std::endl;


        if (f.bad()) {
            throw eckit::ReadError(json.asString());
        }

    } else {
                eckit::Log::info() << json << " does not exist" << std::endl;

    }
}

Index::~Index() {
    flush();
}

eckit::PathName Index::jsonFile() const {
    std::string path = path_;
    return path.substr(0, path.length() - path_.extension().length()) + ".json";
}

void Index::flush() {
    if (files_.changed() || axes_.changed()) {

        eckit::PathName json(jsonFile());

        eckit::Log::info() << "Save " << json << std::endl;


        eckit::FileHandle f(json);

        f.openForWrite(0);
        eckit::AutoClose closer(f);

        std::ostringstream os;

        eckit::JSON j(os);

        j.startObject();
        j << "prefix" << prefix_;
        j << "files";
        files_.json(j);
        j << "axes";
        axes_.json(j);
        j.endObject();

        f.write(os.str().c_str(), os.str().size());
    }
}

void Index::put(const Key &key, const Index::Field &field) {
    axes_.insert(key);
    put_(key, field);
}


//-----------------------------------------------------------------------------

void Index::Field::load(std::istream &s) {
    std::string spath;
    long long offset;
    long long length;
    s >> spath >> offset >> length;
    path_    = spath;
    offset_  = offset;
    length_  = length;
}

void Index::Field::dump(std::ostream &s) const {
    s << path_ << " " << offset_ << " " << length_;
}

//-----------------------------------------------------------------------------

const Key &Index::key() const {
    return key_;
}

//-----------------------------------------------------------------------------


namespace {
eckit::Mutex *local_mutex = 0;
std::map<std::string, IndexFactory *> *m = 0;
pthread_once_t once = PTHREAD_ONCE_INIT;
void init() {
    local_mutex = new eckit::Mutex();
    m = new std::map<std::string, IndexFactory *>();
}
}

IndexFactory::IndexFactory(const std::string &name) :
    name_(name) {

    pthread_once(&once, init);

    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    ASSERT(m->find(name) == m->end());
    (*m)[name] = this;
}

IndexFactory::~IndexFactory() {
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);
    m->erase(name_);
}

void IndexFactory::list(std::ostream &out) {

    pthread_once(&once, init);

    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    const char *sep = "";
    for (std::map<std::string, IndexFactory *>::const_iterator j = m->begin(); j != m->end(); ++j) {
        out << sep << (*j).first;
        sep = ", ";
    }
}


const IndexFactory &IndexFactory::findFactory(const std::string &name) {

    pthread_once(&once, init);

    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    eckit::Log::info() << "Looking for IndexFactory [" << name << "]" << std::endl;

    std::map<std::string, IndexFactory *>::const_iterator j = m->find(name);
    if (j == m->end()) {
        eckit::Log::error() << "No IndexFactory for [" << name << "]" << std::endl;
        eckit::Log::error() << "IndexFactories are:" << std::endl;
        for (j = m->begin() ; j != m->end() ; ++j)
            eckit::Log::error() << "   " << (*j).first << std::endl;
        throw eckit::SeriousBug(std::string("No IndexFactory called ") + name);
    }

    return *(*j).second;
}


Index *IndexFactory::build(const std::string &name,
                           const Key &key, const eckit::PathName &path, Index::Mode mode) {

    const IndexFactory &factory( findFactory(name) );

    return factory.make(key, path, mode);
}

} // namespace fdb5

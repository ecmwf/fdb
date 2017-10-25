/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <vector>

#include "eckit/log/Timer.h"
#include "eckit/log/Plural.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/Seconds.h"
#include "eckit/log/Progress.h"

#include "metkit/MarsParser.h"
#include "metkit/MarsExpension.h"
#include "metkit/MarsRequest.h"

#include "marslib/EmosFile.h"

#include "fdb5/LibFdb.h"
#include "fdb5/config/UMask.h"
#include "fdb5/grib/GribArchiver.h"
#include "fdb5/database/ArchiveVisitor.h"

namespace fdb5 {

using eckit::Log;

//----------------------------------------------------------------------------------------------------------------------

GribArchiver::GribArchiver(const fdb5::Key& key, bool completeTransfers, bool verbose) :
    Archiver(),
    GribDecoder(),
    key_(key),
    completeTransfers_(completeTransfers),
    verbose_(verbose)
{
}

static std::vector<metkit::MarsRequest> str_to_request(const std::string& str) {

    fdb5::Key filter(str);

    if(str.empty()) return std::vector<metkit::MarsRequest>();

    // parse requests

    Log::debug<LibFdb>() << "Parsing request string : " << str << std::endl;

    std::ifstream in(str.c_str());
    metkit::MarsParser parser(in);

    std::vector<metkit::MarsRequest> p = parser.parse();

    Log::debug<LibFdb>() << "Parsed requests:" << std::endl;
    for (std::vector<metkit::MarsRequest>::const_iterator j = p.begin(); j != p.end(); ++j) {
      (*j).dump(Log::debug<LibFdb>());
    }

    // expand requests

    bool inherit = true;
    metkit::MarsExpension expand(inherit);

    std::vector<metkit::MarsRequest> v = expand.expand(p);

    Log::debug<LibFdb>() << "Expanded requests:" << std::endl;
    for (std::vector<metkit::MarsRequest>::const_iterator j = v.begin(); j != v.end(); ++j) {
        (*j).dump(Log::debug<LibFdb>());
    }

    // filter out the keys from that request

    std::set<std::string> keys = filter.keys();

    std::vector<metkit::MarsRequest> r;
    for (std::vector<metkit::MarsRequest>::const_iterator j = v.begin(); j != v.end(); ++j) {
        r.push_back(j->subset(keys));
    }

    return r;
}

void GribArchiver::filters(const std::string& include, const std::string& exclude) {

    include_ = Key(include);
    exclude_ = Key(exclude);
}

bool GribArchiver::filterOut(const Key& k) const {

    if(!k.match(include_)) {
        logVerbose() << "Include key " << include_ << " filtered out datum " << k << std::endl;
        return true;
    }

    if(exclude_.size() and k.match(exclude_)) {
        logVerbose() << "Exclude key " << exclude_ << " filtered out datum " << k << std::endl;
        return true;
    }

    return false; // datum wasn't filtered out
}

eckit::Channel& GribArchiver::logVerbose() const {

    return verbose_ ? Log::info() : Log::debug<LibFdb>();

}

eckit::Length GribArchiver::archive(eckit::DataHandle &source) {

    fdb5::UMask umask(fdb5::UMask::defaultUMask());

    eckit::Timer timer("fdb::service::archive");

    EmosFile file(source);
    size_t len = 0;

    size_t count = 0;
    size_t total_size = 0;

    eckit::Progress progress("FDB archive", 0, file.length());

    try {

        Key key;

        while ( (len = gribToKey(file, key)) ) {

            ASSERT(key.match(key_));

            if( filterOut(key) ) continue;

            logVerbose() << "Archiving " << key << std::endl;

            ArchiveVisitor visitor(*this, key, static_cast<const void *>(buffer()), len);

            this->Archiver::archive(key, visitor);

            total_size += len;
            count++;
            progress(total_size);

            // flush();
        }

    } catch (...) {

        if (completeTransfers_) {
            eckit::Log::error() << "Exception received. Completing transfer." << std::endl;
            // Consume rest of datahandle otherwise client retries for ever
            eckit::Buffer buffer(80 * 1024 * 1024);
            while ( (len = size_t( file.readSome(buffer)) ) ) { /* empty */ }
        }
        throw;
    }

    eckit::Log::userInfo() << "Archived " << eckit::Plural(count, "field") << std::endl;

    eckit::Log::info() << "FDB archive " << eckit::Plural(count, "field") << ","
                       << " size " << eckit::Bytes(total_size) << ","
                       << " in " << eckit::Seconds(timer.elapsed())
                       << " (" << eckit::Bytes(total_size, timer) << ")" <<  std::endl;

    return total_size;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

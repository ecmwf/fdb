/*
 * (C) Copyright 1996- ECMWF.
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

GribArchiver::GribArchiver(const fdb5::Key& key, bool completeTransfers, bool verbose, const Config& config) :
    GribDecoder(),
    fdb_(config),
    key_(key),
    completeTransfers_(completeTransfers),
    verbose_(verbose)
{
}


static std::vector<metkit::MarsRequest> str_to_requests(const std::string& str) {

    // parse requests

    std::string rs = std::string("retrieve,")  + str;

    Log::debug<LibFdb>() << "Parsing request string : " << rs << std::endl;

    std::istringstream in(rs);
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

    return v;
}

static std::vector<metkit::MarsRequest> make_filter_requests(const std::string& str) {

    if(str.empty()) return std::vector<metkit::MarsRequest>();

    std::set<std::string> keys = fdb5::Key(str).keys(); //< keys to filter from that request

    std::vector<metkit::MarsRequest> v = str_to_requests(str);

    std::vector<metkit::MarsRequest> r;
    for (std::vector<metkit::MarsRequest>::const_iterator j = v.begin(); j != v.end(); ++j) {
        r.push_back(j->subset(keys));
        r.back().dump(Log::debug<LibFdb>());
    }

    return r;
}

void GribArchiver::filters(const std::string& include, const std::string& exclude) {

    include_ = make_filter_requests(include);
    exclude_ = make_filter_requests(exclude);

}

static bool matchAny(const metkit::MarsRequest& f, const std::vector<metkit::MarsRequest>& v) {
    for (std::vector<metkit::MarsRequest>::const_iterator r = v.begin(); r != v.end(); ++r) {
        if(f.matches(*r)) return true;
    }
    return false;
}

bool GribArchiver::filterOut(const Key& k) const {

    const bool out = true;

    metkit::MarsRequest field;
    for (Key::const_iterator j = k.begin(); j != k.end(); ++j) {
        eckit::StringList s;
        s.push_back(j->second);
        field.values(j->first, s);
    }

    // filter includes

    if(include_.size() && not matchAny(field, include_)) return out;

    // filter excludes

    if(exclude_.size() && matchAny(field, exclude_)) return out;

    // datum wasn't filtered out

    return !out;
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

            fdb_.archive(key, static_cast<const void *>(buffer()), len);

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

void GribArchiver::flush() {
    fdb_.flush();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

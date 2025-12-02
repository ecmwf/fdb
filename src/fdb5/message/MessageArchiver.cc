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

#include "eckit/log/Bytes.h"
#include "eckit/log/Plural.h"
#include "eckit/log/Progress.h"
#include "eckit/log/Seconds.h"
#include "eckit/log/Timer.h"

#include "eckit/utils/Tokenizer.h"

#include "eckit/message/Message.h"
#include "eckit/message/Reader.h"

#include "metkit/mars/MarsExpansion.h"
#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsRequest.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/ArchiveVisitor.h"
#include "fdb5/message/MessageArchiver.h"

// For HAVE_FAIL_ON_CCSDS
#include "metkit/metkit_config.h"

namespace fdb5 {

using eckit::Log;

//----------------------------------------------------------------------------------------------------------------------
namespace {

std::vector<metkit::mars::MarsRequest> str_to_requests(const std::string& str) {

    // parse requests

    std::string rs = std::string("retrieve,") + str;

    LOG_DEBUG_LIB(LibFdb5) << "Parsing request string : " << rs << std::endl;

    std::istringstream in(rs);
    metkit::mars::MarsParser parser(in);

    std::vector<metkit::mars::MarsParsedRequest> p = parser.parse();

    LOG_DEBUG_LIB(LibFdb5) << "Parsed requests:" << std::endl;
    for (auto j = p.begin(); j != p.end(); ++j) {
        j->dump(Log::debug<LibFdb5>());
    }

    // expand requests

    bool inherit = true;
    metkit::mars::MarsExpansion expand(inherit);

    std::vector<metkit::mars::MarsRequest> v = expand.expand(p);

    LOG_DEBUG_LIB(LibFdb5) << "Expanded requests:" << std::endl;
    for (auto j = v.begin(); j != v.end(); ++j) {
        j->dump(Log::debug<LibFdb5>());
    }

    return v;
}

std::vector<metkit::mars::MarsRequest> make_filter_requests(const std::string& str) {

    if (str.empty()) {
        return {};
    }

    std::set<std::string> keys = Key::parse(str).keys();  //< keys to filter from that request

    std::vector<metkit::mars::MarsRequest> v = str_to_requests(str);

    std::vector<metkit::mars::MarsRequest> r;
    for (auto j = v.begin(); j != v.end(); ++j) {
        r.push_back(j->subset(keys));
        r.back().dump(Log::debug<LibFdb5>());
    }

    return r;
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

MessageArchiver::MessageArchiver(const fdb5::Key& key, bool completeTransfers, bool verbose, const Config& config) :
    MessageDecoder(), fdb_(config), key_(key), completeTransfers_(completeTransfers), verbose_(verbose) {}

void MessageArchiver::setFilters(const std::string& include, const std::string& exclude) {
    include_ = make_filter_requests(include);
    exclude_ = make_filter_requests(exclude);
}

void MessageArchiver::setModifiers(const fdb5::Key& key) {
    modifiers_.reserve(key.size());
    for (const auto& n : key.names()) {
        modifiers_.emplace_back(n, key.get(n));
    }
}

void MessageArchiver::transform(eckit::message::Message& msg) {
    msg.transform(modifiers_);
}

static bool matchAny(const metkit::mars::MarsRequest& f, const std::vector<metkit::mars::MarsRequest>& v) {
    for (auto r = v.begin(); r != v.end(); ++r) {
        if (f.matches(*r)) {
            return true;
        }
    }
    return false;
}

bool MessageArchiver::filterOut(const Key& k) const {

    const bool out = true;

    metkit::mars::MarsRequest field;
    for (Key::const_iterator j = k.begin(); j != k.end(); ++j) {
        eckit::StringList s;
        s.push_back(j->second);
        field.values(j->first, s);
    }

    // filter includes

    if (include_.size() && not matchAny(field, include_)) {
        return out;
    }

    // filter excludes

    if (exclude_.size() && matchAny(field, exclude_)) {
        return out;
    }

    // datum wasn't filtered out

    return !out;
}

eckit::Length MessageArchiver::archive(eckit::DataHandle& source) {

    std::optional<eckit::Timer> timer;
    if (verbose_) {
        timer.emplace("fdb::service::archive");
    }

    eckit::message::Reader reader(source);

    size_t count = 0;
    size_t total_size = 0;

    eckit::Progress progress("FDB archive", 0, source.estimate());

    try {

        eckit::message::Message msg;

        while ((msg = reader.next())) {

#ifdef metkit_HAVE_FAIL_ON_CCSDS

            if (msg.getString("packingType") == "grid_ccsds") {
                throw eckit::SeriousBug("grid_ccsds is disabled");
            }

#endif

            Key key;

            messageToKey(msg, key);

            LOG_DEBUG_LIB(LibFdb5) << "Archiving message "
                                   << " key: " << key << " length:" << msg.length() << std::endl;

            ASSERT(key.match(key_));

            if (filterOut(key)) {
                continue;
            }

            if (modifiers_.size()) {
                transform(msg);
                key.clear();
                messageToKey(msg, key);  // re-build the key, as it may have changed
            }

            if (verbose_) {
                Log::info() << "Archiving " << key << std::endl;
            }
            else {
                LOG_DEBUG_LIB(LibFdb5) << "Archiving " << key << std::endl;
            }

            fdb_.archive(key, msg.data(), msg.length());

            total_size += msg.length();
            count++;
            progress(total_size);

            // flush();
        }
    }
    catch (...) {

        if (completeTransfers_) {
            eckit::Log::error() << "Exception received. Completing transfer." << std::endl;
            // Consume rest of datahandle otherwise client retries for ever
            while (reader.next()) { /* empty */
            }
        }
        throw;
    }

    eckit::Log::userInfo() << "Archived " << eckit::Plural(count, "message") << std::endl;

    if (verbose_) {
        eckit::Log::info() << "FDB archive " << eckit::Plural(count, "message") << ","
                           << " size " << eckit::Bytes(total_size) << ","
                           << " in " << eckit::Seconds(timer->elapsed()) << " ("
                           << eckit::Bytes(total_size, timer.value()) << ")" << std::endl;
    }

    return total_size;
}

void MessageArchiver::flush() {
    fdb_.flush();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

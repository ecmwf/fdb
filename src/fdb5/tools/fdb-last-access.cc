/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>
#include <ctime>
#include <memory>
#include <string>
#include <vector>

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/log/JSON.h"
#include "eckit/log/Log.h"
#include "eckit/log/Plural.h"
#include "eckit/log/TimeStamp.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/utils/Translator.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/StatusIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/tools/FDBVisitTool.h"

using namespace eckit;
using namespace eckit::option;

namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------

namespace {

/// Parse a duration of the form <n>[s|m|h|d|w]. A bare number is seconds.
time_t parseDuration(const std::string& spec) {

    if (spec.empty()) {
        throw UserError("Empty duration", Here());
    }

    const char suffix = spec.back();
    std::string digits = spec;
    time_t multiplier = 1;

    switch (suffix) {
        case 's': multiplier = 1; break;
        case 'm': multiplier = 60; break;
        case 'h': multiplier = 60 * 60; break;
        case 'd': multiplier = 24 * 60 * 60; break;
        case 'w': multiplier = 7 * 24 * 60 * 60; break;
        default:
            if (!::isdigit(static_cast<unsigned char>(suffix))) {
                throw UserError("Unrecognised duration suffix in '" + spec + "'; expected one of s, m, h, d, w",
                                Here());
            }
            break;
    }

    if (multiplier != 1 || !::isdigit(static_cast<unsigned char>(suffix))) {
        digits = spec.substr(0, spec.size() - 1);
    }

    for (char c : digits) {
        if (!::isdigit(static_cast<unsigned char>(c))) {
            throw UserError("Malformed duration '" + spec + "'", Here());
        }
    }

    return eckit::Translator<std::string, long>()(digits) * multiplier;
}

/// A database with no timestamp - one written by an older FDB, say - has an access history that
/// is unknown. That is emphatically not the same as "not accessed recently", and is reported as
/// such wherever it appears so that nothing downstream can mistake it for a cold dataset.
constexpr const char* unknown = "UNKNOWN";

std::string accessTime(time_t t) {
    return t == 0 ? std::string(unknown) : std::string(eckit::TimeStamp(t));
}

/// Clamped: a timestamp in the future (clock skew on a shared filesystem) is simply not old.
time_t age(time_t when, time_t now) {
    return now > when ? now - when : 0;
}

const std::vector<std::string> wipeMinimumKeys{"class", "expver", "stream", "date", "time"};

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

class FDBLastAccess : public FDBVisitTool {
public:  // methods

    FDBLastAccess(int argc, char** argv) : FDBVisitTool(argc, argv, "") {
        options_.push_back(new SimpleOption<std::string>(
            "older-than", "Only report databases not accessed within this period, e.g. 90d, 12h"));
        options_.push_back(new SimpleOption<std::string>(
            "newer-than", "Only report databases accessed within this period, e.g. 7d"));
        options_.push_back(new SimpleOption<bool>(
            "unknown", "Only report databases with no access marker. These have UNKNOWN access "
                       "history and must never be treated as cold"));
        options_.push_back(new SimpleOption<bool>(
            "read", "Consider only the read timestamp. May not be combined with --write"));
        options_.push_back(new SimpleOption<bool>(
            "write", "Consider only the write timestamp. May not be combined with --read"));
        options_.push_back(new SimpleOption<bool>("simple", "Tabular output: '<age in seconds> <request>'"));
        options_.push_back(new SimpleOption<bool>("json", "JSON serialised output for easy downstream parsing"));
        options_.push_back(new SimpleOption<bool>(
            "wipe", "Wipe the reported databases. Requires --older-than; a dry run without --doit"));
        options_.push_back(new SimpleOption<bool>("doit", "With --wipe, actually delete the data"));
    }

private:  // methods

    void execute(const CmdArgs& args) override;
    void init(const CmdArgs& args) override;
    void finish(const CmdArgs& args) override;

    /// Which of the two timestamps a query selects on.
    enum class Access { Read, Write, MostRecent };

    /// The selected timestamp, or 0 if it is not recorded.
    time_t selected(const StatusElement& elem) const;
    bool report(const StatusElement& elem, time_t now) const;

    void printDefault(const StatusElement& elem, time_t now) const;
    void printSimple(const StatusElement& elem, time_t now) const;
    void printJSON(eckit::JSON& json, const StatusElement& elem, time_t now) const;

    void wipeDatabase(FDB& fdb, const StatusElement& elem);

    time_t olderThan_{0};
    time_t newerThan_{0};
    bool haveOlderThan_{false};
    bool haveNewerThan_{false};
    bool unknownOnly_{false};
    bool simple_{false};
    bool json_{false};
    bool wipe_{false};
    bool doit_{false};
    size_t wiped_{0};
    Access access_{Access::MostRecent};
};

//----------------------------------------------------------------------------------------------------------------------

void FDBLastAccess::init(const CmdArgs& args) {

    FDBVisitTool::init(args);

    unknownOnly_ = args.getBool("unknown", unknownOnly_);
    simple_ = args.getBool("simple", simple_);
    json_ = args.getBool("json", json_);

    if (simple_ && json_) {
        throw UserError("--simple and --json are alternative output formats; specify at most one", Here());
    }

    const bool read = args.getBool("read", false);
    const bool write = args.getBool("write", false);

    if (read && write) {
        throw UserError("--read and --write cannot be combined", Here());
    }
    access_ = read ? Access::Read : (write ? Access::Write : Access::MostRecent);

    if (args.has("older-than")) {
        olderThan_ = parseDuration(args.getString("older-than"));
        haveOlderThan_ = true;
    }
    if (args.has("newer-than")) {
        newerThan_ = parseDuration(args.getString("newer-than"));
        haveNewerThan_ = true;
    }

    if (haveOlderThan_ && haveNewerThan_ && newerThan_ > olderThan_) {
        throw UserError("--newer-than must be shorter than --older-than; otherwise nothing can match", Here());
    }

    if (unknownOnly_ && (haveOlderThan_ || haveNewerThan_)) {
        throw UserError("--unknown cannot be combined with --older-than or --newer-than", Here());
    }

    wipe_ = args.getBool("wipe", wipe_);
    doit_ = args.getBool("doit", doit_);

    if (doit_ && !wipe_) {
        throw UserError("--doit commits a --wipe, and has no meaning on its own", Here());
    }

    if (wipe_) {

        if (simple_ || json_) {
            throw UserError("--wipe cannot be combined with --simple or --json", Here());
        }

        if (unknownOnly_) {
            throw UserError("--wipe cannot be safely used against data with know known age information", Here());
        }

        if (!haveOlderThan_) {
            throw UserError("--wipe must be bounded by --older-than", Here());
        }

        if (haveNewerThan_) {
            throw UserError("--wipe cannot be combined with --newer-than", Here());
        }
    }
}

/// The timestamp this query selects on. With neither --read nor --write, the more recent of the
/// two, so --older-than selects only databases that are cold on both counts.
time_t FDBLastAccess::selected(const StatusElement& elem) const {
    switch (access_) {
        case Access::Read:
            return elem.lastRead;
        case Access::Write:
            return elem.lastWrite;
        case Access::MostRecent:
            return std::max(elem.lastRead, elem.lastWrite);
    }
    return 0;
}

bool FDBLastAccess::report(const StatusElement& elem, time_t now) const {

    const time_t when = selected(elem);

    if (when == 0) {
        return unknownOnly_ || !(haveOlderThan_ || haveNewerThan_);
    }

    if (unknownOnly_) {
        return false;
    }

    const time_t elapsed = age(when, now);

    if (haveOlderThan_ && elapsed < olderThan_) {
        return false;
    }
    if (haveNewerThan_ && elapsed > newerThan_) {
        return false;
    }

    return true;
}

/// The human readable form. A database with no record is reported as UNKNOWN rather than being
/// given a fabricated age, and the block keeps its shape so that output stays easy to scan.
void FDBLastAccess::printDefault(const StatusElement& elem, time_t now) const {

    const time_t when = selected(elem);

    Log::info() << "  database:   " << elem.key << std::endl
                << "  location:   " << elem.location.asString() << std::endl
                << "  last read:  " << accessTime(elem.lastRead) << std::endl
                << "  last write: " << accessTime(elem.lastWrite) << std::endl;

    if (when == 0) {
        Log::info() << "  age:        " << unknown << std::endl;
    }
    else {
        Log::info() << "  age:        " << age(when, now) / (24 * 60 * 60) << " days" << std::endl;
    }
}

/// The tabular form: the age in seconds, then the matching request.
void FDBLastAccess::printSimple(const StatusElement& elem, time_t now) const {

    const time_t when = selected(elem);

    if (when == 0) {
        Log::info() << unknown << " " << elem.key << std::endl;
    }
    else {
        Log::info() << age(when, now) << " " << elem.key << std::endl;
    }
}

void FDBLastAccess::printJSON(eckit::JSON& json, const StatusElement& elem, time_t now) const {

    const time_t when = selected(elem);

    json.startObject();

    json << "age";
    if (when == 0) {
        json << unknown;
    }
    else {
        json << static_cast<long long>(age(when, now));
    }

    json << "lastRead" << static_cast<long long>(elem.lastRead);
    json << "lastWrite" << static_cast<long long>(elem.lastWrite);
    json << "location" << elem.location.asString();

    // Schema order, not the alphabetical order a std::map would stream in.
    json << "key";
    json.startObject();
    for (const auto& name : elem.key.names()) {
        json << name << elem.key.get(name);
    }
    json.endObject();

    json.endObject();
}

void FDBLastAccess::wipeDatabase(FDB& fdb, const StatusElement& elem) {

    // An empty key would make an empty request, which matches the entire archive. This cannot
    // happen with a key that came from a catalogue, which is exactly why it is worth saying so.
    if (elem.key.empty()) {
        throw SeriousBug("Refusing to wipe: the reported database carried no key", Here());
    }

    const FDBToolRequest request(elem.key.request(), false, wipeMinimumKeys);

    auto it = fdb.wipe(request, doit_, false, false);

    WipeElement wipeElem;
    while (it.next(wipeElem)) {
        Log::info() << wipeElem;
    }

    wiped_++;
}

void FDBLastAccess::execute(const CmdArgs& args) {

    FDB fdb(config(args));

    const time_t now = ::time(nullptr);

    std::unique_ptr<eckit::JSON> json;
    if (json_) {
        json = std::make_unique<eckit::JSON>(Log::info());
        json->startList();
    }

    for (const FDBToolRequest& request : requests("read")) {

        auto statusIterator = fdb.status(request);

        size_t visited = 0;
        std::vector<StatusElement> toWipe;

        StatusElement elem;
        while (statusIterator.next(elem)) {

            visited++;

            if (!report(elem, now)) {
                continue;
            }

            if (json) {
                printJSON(*json, elem, now);
            }
            else if (simple_) {
                printSimple(elem, now);
            }
            else {
                printDefault(elem, now);
            }

            if (wipe_) {
                toWipe.push_back(elem);
            }
        }

        if (visited == 0 && fail()) {
            std::ostringstream ss;
            ss << "No FDB entries found for: " << request << std::endl;
            throw FDBToolException(ss.str());
        }

        for (const StatusElement& candidate : toWipe) {
            wipeDatabase(fdb, candidate);
        }
    }

    if (json) {
        json->endList();
    }
}

void FDBLastAccess::finish(const CmdArgs&) {

    if (!wipe_) {
        return;
    }

    // n.b. the space belongs to the verb, not the noun: Plural appends its 's' to the string it is
    // given, so "database " would pluralise as "2 database s".
    Log::info() << newl << Plural(wiped_, "database") << (doit_ ? " wiped" : " would be wiped") << std::endl;

    if (!doit_) {
        Log::info() << "Rerun command with --doit flag to delete the data" << std::endl << std::endl;
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char** argv) {
    fdb5::tools::FDBLastAccess app(argc, argv);
    return app.start();
}

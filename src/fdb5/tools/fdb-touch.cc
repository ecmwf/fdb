/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cstring>
#include <ctime>
#include <string>

#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/AccessIterator.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/StatusIterator.h"
#include "fdb5/tools/FDBVisitTool.h"

using namespace eckit;
using namespace eckit::option;

namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBTouch : public FDBVisitTool {
public:  // methods

    FDBTouch(int argc, char** argv) : FDBVisitTool(argc, argv, "class,expver,stream,date,time") {
        options_.push_back(new SimpleOption<bool>("read", "Update read timestamp"));
        options_.push_back(new SimpleOption<bool>("write", "Update write timestamp"));
        options_.push_back(new SimpleOption<std::string>(
            "time", "Use the specified time than the current time. YYYYMMDD, YYYY-MM-DD, "
                    "YYYYMMDDHHMMSS or YYYY-MM-DDTHH:MM:SS, interpreted as UTC"));
        options_.push_back(new SimpleOption<bool>(
            "from-toc", "Use the mtime of the database's toc, rather than the "
                        "current time"));
    }

private:  // methods

    void execute(const CmdArgs& args) override;
    void init(const CmdArgs& args) override;

    bool read_{false};
    bool write_{false};
    AccessTime when_;
};

//----------------------------------------------------------------------------------------------------------------------

namespace {

/// Parse the accepted date/datetime forms as UTC.
///
/// n.b. deliberately not eckit::DateTime(const std::string&), which ASSERTs unless handed exactly
/// two space-separated tokens - an abort rather than a diagnosable error on a mistyped flag. And
/// deliberately no bare epoch-seconds form: a ten-digit number is ambiguous against YYYYMMDDHH.
time_t parseTime(const std::string& spec) {

    static const char* formats[] = {"%Y%m%d%H%M%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y%m%d", "%Y-%m-%d"};

    for (const char* format : formats) {
        struct tm tm;
        ::memset(&tm, 0, sizeof(tm));
        const char* end = ::strptime(spec.c_str(), format, &tm);
        if (end != nullptr && *end == '\0') {
            return ::timegm(&tm);
        }
    }

    throw UserError("Cannot parse --time=" + spec +
                        "; expected YYYYMMDD, YYYY-MM-DD, YYYYMMDDHHMMSS or YYYY-MM-DDTHH:MM:SS",
                    Here());
}

}  // namespace

void FDBTouch::init(const CmdArgs& args) {

    FDBVisitTool::init(args);

    read_ = args.getBool("read", read_);
    write_ = args.getBool("write", write_);

    if (!(read_ || write_)) {
        throw UserError("Nothing to do: specify --read or --write", Here());
    }

    const bool fromToc = args.getBool("from-toc", false);

    if (fromToc && args.has("time")) {
        throw UserError("--time and --from-toc both choose which instant to record; specify one", Here());
    }

    if (fromToc) {
        when_ = AccessTime::catalogueTime();
    }
    else if (args.has("time")) {
        when_ = AccessTime::at(parseTime(args.getString("time")));
    }
}

void FDBTouch::execute(const CmdArgs& args) {

    FDB fdb(config(args));

    AccessIdentifiers identifiers;
    if (read_) {
        identifiers |= AccessIdentifier::Read;
    }
    if (write_) {
        identifiers |= AccessIdentifier::Write;
    }

    for (const FDBToolRequest& request : requests("read")) {

        auto it = fdb.touch(request, identifiers, when_);

        size_t count = 0;
        StatusElement elem;
        while (it.next(elem)) {
            Log::info() << "Touched: " << elem.key << std::endl;
            count++;
        }

        if (count == 0 && fail()) {
            std::ostringstream ss;
            ss << "No FDB entries found for: " << request << std::endl;
            throw FDBToolException(ss.str());
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char** argv) {
    fdb5::tools::FDBTouch app(argc, argv);
    return app.start();
}

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "eckit/exception/Exceptions.h"
#include "eckit/log/BigNum.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/JSON.h"
#include "eckit/log/Log.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "fdb5/LibFdb5.h"
#include "metkit/mars/MarsRequest.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/tools/FDBVisitTool.h"

using namespace eckit;
using namespace eckit::option;

namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBList : public FDBVisitTool {
public:  // methods

    FDBList(int argc, char** argv) : FDBVisitTool(argc, argv, "class,expver") {
        options_.push_back(new SimpleOption<bool>("location", "Also print the location of each field"));
        options_.push_back(new SimpleOption<bool>("timestamp", "Also print the timestamp when the field was indexed"));
        options_.push_back(new SimpleOption<bool>("length", "Also print the field size"));
        options_.push_back(new SimpleOption<bool>("full", "Include all entries (including masked duplicates)"));
        options_.push_back(new SimpleOption<bool>("only-duplicates", "List only the duplicated (older) entries"));
        options_.push_back(
            new SimpleOption<bool>("porcelain",
                                   "Streamlined and stable output. Useful as input for other tools or scripts."
                                   "Incompatible with timestamp"));
        options_.push_back(new SimpleOption<bool>("json", "Output available fields in JSON form"));
        options_.push_back(new SimpleOption<bool>("compact", "Aggregate available fields in MARS requests"));
        options_.push_back(new SimpleOption<long>("depth", "Output entries up to 'depth' levels deep [1-3]"));
    }

private:  // methods

    void execute(const CmdArgs& args) override;
    void init(const CmdArgs& args) override;

    bool location_{false};
    bool timestamp_{false};
    bool length_{false};
    ListMode listMode_{ListMode::Deduplicate};
    bool porcelain_{false};
    bool json_{false};
    bool compact_{false};
    int depth_{3};
};

//----------------------------------------------------------------------------------------------------------------------

void FDBList::init(const CmdArgs& args) {

    FDBVisitTool::init(args);

    location_                 = args.getBool("location", location_);
    timestamp_                = args.getBool("timestamp", timestamp_);
    length_                   = args.getBool("length", length_);
    const bool full           = args.getBool("full", false);
    const bool onlyDuplicates = args.getBool("only-duplicates", false);
    porcelain_                = args.getBool("porcelain", porcelain_);
    json_                     = args.getBool("json", json_);
    compact_                  = args.getBool("compact", compact_);
    depth_                    = args.getInt("depth", depth_);

    ASSERT(depth_ > 0 && depth_ < 4);

    if (json_) {
        eckit::Log::debug<LibFdb5>() << "Setting porcelain=true" << '\n';
        porcelain_ = true;
    }

    if (porcelain_) {
        if (timestamp_) {
            throw UserError("--porcelain and --timestamp are not compatible", Here());
        }
    }

    if (compact_) {
        if (location_) {
            throw UserError("--compact and --location are not compatible", Here());
        }
        if (full) {
            throw UserError("--compact and --full are not compatible", Here());
        }
        if (onlyDuplicates) {
            throw UserError("--compact and --only-duplicates are not compatible", Here());
        }
    }

    if (full && onlyDuplicates) {
        throw UserError("--full and --only-duplicates are not compatible", Here());
    }

    if (onlyDuplicates) {
        listMode_ = ListMode::OnlyDuplicates;
    }
    else if (!full && !compact_) {
        listMode_ = ListMode::Deduplicate;
    }
    else {
        listMode_ = ListMode::Full;
    }

    /// @todo option ignore-errors
}

void FDBList::execute(const CmdArgs& args) {

    FDB fdb(config(args));

    std::unique_ptr<JSON> json;
    if (json_) {
        json = std::make_unique<JSON>(Log::info());
        json->startList();
    }

    for (const FDBToolRequest& request : requests("list")) {

        if (!porcelain_) {
            Log::info() << "Listing for request" << std::endl;
            request.print(Log::info());
            Log::info() << std::endl;
        }

        auto listObject = fdb.list(request, listMode_, depth_);

        if (compact_) {
            auto [fields, total] = listObject.dumpCompact(Log::info());
            if (!porcelain_) {
                Log::info() << "Entries       : " << eckit::BigNum(fields) << std::endl;
                Log::info() << "Total         : " << eckit::BigNum(total) << " (" << eckit::Bytes(total) << ')'
                            << std::endl;
            }
        }
        else {
            ListElement elem;
            while (listObject.next(elem)) {

                // JSON output
                if (json) {
                    *json << elem;
                    continue;
                }

                elem.print(Log::info(), location_, length_, timestamp_, ", ");
                Log::info() << std::endl;

            }  // while
            // n.b. finding no data is not an error for fdb-list
        }
    }  // requests

    if (json) {
        json->endList();
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char** argv) {
    fdb5::tools::FDBList app(argc, argv);
    return app.start();
}

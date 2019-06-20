/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "fdb5/tools/FDBVisitTool.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/LibFdb5.h"

using namespace eckit;
using namespace eckit::option;

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBWipe : public FDBVisitTool {

public: // methods

    FDBWipe(int argc, char **argv) :
        FDBVisitTool(argc, argv, "class,expver,stream,date,time"),
        doit_(false),
        ignoreNoData_(false),
        porcelain_(false) {

        options_.push_back(new SimpleOption<bool>("doit", "Delete the files (data and indexes)"));
        options_.push_back(new SimpleOption<bool>("ignore-no-data", "No data available to delete is not an error"));
        options_.push_back(new SimpleOption<bool>("porcelain", "List only the deleted files"));
    }

private: // methods

    std::ostream& alwaysLog() { return Log::info(); }
    std::ostream& verboseLog() { return porcelain_ ? Log::debug<LibFdb5>() : Log::info(); }

    virtual void usage(const std::string &tool) const;
    virtual void init(const CmdArgs &args);
    virtual void execute(const CmdArgs& args);
    virtual void finish(const CmdArgs &args);

private: // members
    bool doit_;
    bool ignoreNoData_;
    bool porcelain_;
};

void FDBWipe::usage(const std::string &tool) const {

    Log::info() << std::endl
                << "Usage: " << tool << " [options] [DB request]" << std::endl
                << std::endl
                << std::endl
                << "Examples:" << std::endl
                << "=========" << std::endl << std::endl
                << tool << " class=rd,expver=xywz,stream=oper,date=20190603,time=00"
                << std::endl
                << std::endl;

    FDBTool::usage(tool);
}

void FDBWipe::init(const CmdArgs &args) {

    FDBVisitTool::init(args);

    args.get("doit", doit_);
    args.get("ignore-no-data", ignoreNoData_);
    args.get("porcelain", porcelain_);
}

void FDBWipe::execute(const CmdArgs& args) {

    FDB fdb;

    for (const FDBToolRequest& request : requests()) {

        verboseLog() << "Wiping for request" << std::endl;
        request.print(verboseLog());
        verboseLog() << std::endl;

        auto listObject = fdb.wipe(request, doit_, !porcelain_);

        size_t count = 0;
        WipeElement elem;
        while (listObject.next(elem)) {

            verboseLog() << "FDB owner: " << elem.owner << std::endl
                        << std::endl;

            verboseLog() << "Metadata files to delete:" << std::endl;
            for (const auto& f : elem.metadataPaths) {
                verboseLog() << "    ";
                alwaysLog() << f << std::endl;
            }
            verboseLog() << std::endl;

            verboseLog() << "Data files to delete: " << std::endl;
            if (elem.dataPaths.empty()) verboseLog() << " - NONE -" << std::endl;
            for (const auto& f : elem.dataPaths) {
                verboseLog() << "    ";
                alwaysLog() << f << std::endl;
            }
            verboseLog() << std::endl;

            verboseLog() << "Untouched files:" << std::endl;
            if (elem.safePaths.empty()) verboseLog() << " - NONE - " << std::endl;
            for (const auto& f : elem.safePaths) {
                verboseLog() << "    " << f << std::endl;
            }
            verboseLog() << std::endl;

            if (!elem.safePaths.empty()) {
                verboseLog() << "Indexes to mask:" << std::endl;
                if (elem.indexes.empty()) verboseLog() << " - NONE - " << std::endl;
                for (const auto& f : elem.indexes) {
                    verboseLog() << "    " << *f << std::endl;
                }
            }

            count++;
        }

        if (count == 0 && !ignoreNoData_ && fail()) {
            std::ostringstream ss;
            ss << "No FDB entries found for: " << request << std::endl;
            throw FDBToolException(ss.str());
        }
    }
}


void FDBWipe::finish(const CmdArgs&) {

    if (!doit_ && !porcelain_) {
        Log::info() << std::endl
                    << "Rerun command with --doit flag to delete unused files"
                    << std::endl
                    << std::endl;
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5

int main(int argc, char **argv) {
    fdb5::tools::FDBWipe app(argc, argv);
    return app.start();
}

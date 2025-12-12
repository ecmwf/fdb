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

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBVisitTool.h"

using namespace eckit;
using namespace eckit::option;

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBWipe : public FDBVisitTool {

public:  // methods

    FDBWipe(int argc, char** argv) :
        FDBVisitTool(argc, argv, "class,expver,stream,date,time"),
        doit_(false),
        ignoreNoData_(false),
        porcelain_(false),
        unsafeWipeAll_(false) {

        options_.push_back(new SimpleOption<bool>("doit", "Delete the files (data and indexes)"));
        options_.push_back(new SimpleOption<bool>("ignore-no-data", "No data available to delete is not an error"));
        options_.push_back(new SimpleOption<bool>("porcelain", "List only the deleted files"));
        options_.push_back(
            new SimpleOption<bool>("unsafe-wipe-all", "Wipe all (unowned) contents of an unclean database"));
    }

private:  // methods

    virtual void usage(const std::string& tool) const;
    virtual void init(const CmdArgs& args);
    virtual void execute(const CmdArgs& args);
    virtual void finish(const CmdArgs& args);

private:  // members

    bool doit_;
    bool ignoreNoData_;
    bool porcelain_;
    bool unsafeWipeAll_;
};

void FDBWipe::usage(const std::string& tool) const {

    Log::info() << std::endl
                << "Usage: " << tool << " [options] [DB request]" << std::endl
                << std::endl
                << std::endl
                << "Examples:" << std::endl
                << "=========" << std::endl
                << std::endl
                << tool << " class=rd,expver=xywz,stream=oper,date=20190603,time=00" << std::endl
                << std::endl;
}

void FDBWipe::init(const CmdArgs& args) {

    FDBVisitTool::init(args);

    doit_          = args.getBool("doit", false);
    ignoreNoData_  = args.getBool("ignore-no-data", false);
    porcelain_     = args.getBool("porcelain", false);
    unsafeWipeAll_ = args.getBool("unsafe-wipe-all", false);
}

void FDBWipe::execute(const CmdArgs& args) {

    FDB fdb(config(args));

    for (const FDBToolRequest& request : requests()) {

        if (!porcelain_) {
            Log::info() << "Wiping for request" << std::endl;
            request.print(Log::info());
            Log::info() << std::endl;
        }

        auto iter = fdb.wipe(request, doit_, porcelain_, unsafeWipeAll_);

        size_t count = 0;

        WipeElement elem;
        while (iter.next(elem)) {
            Log::info() << elem;
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
        Log::info() << std::endl << "Rerun command with --doit flag to delete unused files" << std::endl << std::endl;
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace tools
}  // namespace fdb5

int main(int argc, char** argv) {
    fdb5::tools::FDBWipe app(argc, argv);
    return app.start();
}

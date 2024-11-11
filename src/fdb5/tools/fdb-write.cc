/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <memory>

#include "eckit/config/LocalConfiguration.h"
#include "eckit/io/DataHandle.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/log/Log.h"

#include "fdb5/message/MessageArchiver.h"
#include "fdb5/tools/FDBTool.h"


class FDBWrite : public fdb5::FDBTool {

    virtual void usage(const std::string &tool) const;

    virtual void init(const eckit::option::CmdArgs &args);

    virtual int minimumPositionalArguments() const { return 1; }

    virtual void execute(const eckit::option::CmdArgs &args);

public:

    FDBWrite(int argc, char **argv) :
        fdb5::FDBTool(argc, argv),
        verbose_(false) {

        options_.push_back(
                    new eckit::option::SimpleOption<std::string>("include-filter",
                    "List of comma separated key-values of what to include from the input data, e.g --include-filter=stream=enfo,date=10102017"));

        options_.push_back(
                    new eckit::option::SimpleOption<std::string>("exclude-filter",
                    "List of comma separated key-values of what to exclude from the input data, e.g --exclude-filter=time=0000"));

        options_.push_back(
            new eckit::option::SimpleOption<std::string>("modifiers",
                                                         "List of comma separated key-values of modifiers to each message "
                                                         "int input data, e.g --modifiers=packingType=grib_ccsds,expver=0042"));

        options_.push_back(new eckit::option::SimpleOption<bool>("statistics", "Report timing statistics"));

        options_.push_back(new eckit::option::SimpleOption<bool>("verbose", "Print verbose output"));
    }

    std::string filterInclude_;
    std::string filterExclude_;
    std::string modifiers_;
    bool verbose_;
};

void FDBWrite::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl << "Usage: " << tool << " [--filter-include=...] [--filter-exclude=...] <path1> [path2] ..." << std::endl;
    fdb5::FDBTool::usage(tool);
}

void FDBWrite::init(const eckit::option::CmdArgs& args)
{
    FDBTool::init(args);
    args.get("include-filter", filterInclude_);
    args.get("exclude-filter", filterExclude_);
    args.get("modifiers", modifiers_);
    verbose_ = args.getBool("verbose", false);
}

void FDBWrite::execute(const eckit::option::CmdArgs &args) {

    fdb5::MessageArchiver archiver(fdb5::Key(), false, verbose_, config(args));

    archiver.filters(filterInclude_, filterExclude_);
    archiver.modifiers(modifiers_);

    for (size_t i = 0; i < args.count(); i++) {

        eckit::PathName path(args(i));

        eckit::Log::info() << "Processing " << path << std::endl;

        std::unique_ptr<eckit::DataHandle> dh ( path.fileHandle() );

        archiver.archive( *dh );
    }
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBWrite app(argc, argv);
    return app.start();
}


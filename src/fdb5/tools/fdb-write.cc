/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/io/DataHandle.h"
#include "eckit/memory/ScopedPtr.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/VectorOption.h"

#include "fdb5/grib/GribArchiver.h"
#include "fdb5/tools/FDBAccess.h"
#include "fdb5/config/UMask.h"

class FDBWrite : public fdb5::FDBAccess {

    virtual void usage(const std::string &tool) const;

    virtual void init(const eckit::option::CmdArgs &args);

    virtual int minimumPositionalArguments() const { return 1; }

    virtual void execute(const eckit::option::CmdArgs &args);

public:

    FDBWrite(int argc, char **argv) : fdb5::FDBAccess(argc, argv) {

        options_.push_back(
                    new eckit::option::SimpleOption<std::string>("include-filter",
                    "List of comma separated key-values of what to include from the input data, e.g --include-filter=stream=enfo,date=10102017"));

        options_.push_back(
                    new eckit::option::SimpleOption<std::string>("exclude-filter",
                    "List of comma separated key-values of what to exclude from the input data, e.g --exclude-filter=time=0000"));
    }

    std::string filterInclude_;
    std::string filterExclude_;
};

void FDBWrite::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl << "Usage: " << tool << " [--filter-include=...] [--filter-exclude=...] <path1> [path2] ..." << std::endl;
    fdb5::FDBAccess::usage(tool);
}

void FDBWrite::init(const eckit::option::CmdArgs& args)
{
    args.get("include-filter", filterInclude_);
    args.get("exclude-filter", filterExclude_);
}

void FDBWrite::execute(const eckit::option::CmdArgs &args) {

    fdb5::GribArchiver archiver;

    archiver.filters(filterInclude_, filterExclude_);

    for (size_t i = 0; i < args.count(); i++) {

        eckit::PathName path(args(i));

        std::cout << "Processing " << path << std::endl;

        eckit::ScopedPtr<eckit::DataHandle> dh ( path.fileHandle() );

        archiver.archive( *dh );
    }

}


int main(int argc, char **argv) {
    FDBWrite app(argc, argv);
    return app.start();
}


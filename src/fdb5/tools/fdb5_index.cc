/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "eckit/option/CmdArgs.h"
#include "fdb5/grib/GribIndexer.h"
#include "fdb5/tools/FDBAccess.h"


class FDBIndex : public fdb5::FDBAccess {
    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int minimumPositionalArguments() const { return 1; }

public:
    FDBIndex(int argc, char **argv): fdb5::FDBAccess(argc, argv) {}
};

void FDBIndex::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " gribfile1 [gribfile2] ..." << std::endl;
    fdb5::FDBAccess::usage(tool);
}

void FDBIndex::execute(const eckit::option::CmdArgs &args) {

    fdb5::GribIndexer indexer;

    for(size_t i = 0; i < args.count(); i++) {
        eckit::PathName path(args(i));

        std::cout << "Processing " << path << std::endl;

        indexer.index(path);
    }
}


int main(int argc, char **argv) {
    FDBIndex app(argc, argv);
    return app.start();
}


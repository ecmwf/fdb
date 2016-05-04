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
#include "fdb5/legacy/FDBIndexScanner.h"
#include "fdb5/tools/FDBAccess.h"

//----------------------------------------------------------------------------------------------------------------------

class FDBAdopt : public fdb5::FDBAccess {

    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int minimumPositionalArguments() const { return 1; }

public:

    FDBAdopt(int argc, char **argv) :
        fdb5::FDBAccess(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<std::string>("pattern", "Pattern matching the legacy fdb directory, e.g. /*[pcf][fc]:0000:*."));

    }

};

void FDBAdopt::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " [--pattern=/*fc:0000:*.] gribfile1 [gribfile2] ..." << std::endl;
    fdb5::FDBAccess::usage(tool);
}

void FDBAdopt::execute(const eckit::option::CmdArgs &args) {


    std::string pattern = "/:*.";
    args.get("pattern", pattern);

    for (size_t i = 0; i < args.count(); i++) {
        eckit::PathName path(args(i));

        eckit::Log::info() << "Scanning FDB db " << path << std::endl;

        std::vector<eckit::PathName> indexes;

        eckit::PathName::match(path / pattern, indexes, true); // match checks that path is a directory

        for (std::vector<eckit::PathName>::const_iterator j = indexes.begin(); j != indexes.end(); ++j) {
            fdb5::legacy::FDBIndexScanner scanner(*j);
            scanner.execute();
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBAdopt app(argc, argv);
    return app.start();
}


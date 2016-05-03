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
#include "fdb5/tools/FDBTool.h"
#include "fdb5/toc/TocDB.h"

using namespace std;
using namespace eckit;
using namespace fdb5;

//----------------------------------------------------------------------------------------------------------------------

class FDBWhere : public FDBTool {
public: // methods

    FDBWhere(int argc, char **argv) : FDBTool(argc, argv) {
    }

private: // methods

    virtual void run();

    static void usage(const std::string &tool);

};

void FDBWhere::usage(const std::string &tool) {

    eckit::Log::info() << std::endl << "Usage: " << tool << " [path1|request1] [path2|request2] ..." << std::endl;
    FDBTool::usage(tool);
}

void FDBWhere::run() {

    eckit::option::CmdArgs args(&FDBWhere::usage, -1, options_);

    if (args.count() == 0) {
        std::vector<eckit::PathName> roots = TocDB::roots();
        for (std::vector<eckit::PathName>::const_iterator i = roots.begin(); i != roots.end(); ++i) {
            std::cout << *i << std::endl;
        }
    }

    for (size_t i = 0; i < args.count(); ++i) {
        std::cout << databasePath(args.args(i)) << std::endl;
    }

}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBWhere app(argc, argv);
    return app.start();
}

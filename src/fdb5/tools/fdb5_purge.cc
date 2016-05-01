/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/runtime/Context.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/CmdArgs.h"

#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/toc/PurgeVisitor.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/tools/FDBTool.h"

using namespace std;
using namespace eckit;
using namespace eckit::option;
using namespace fdb5;

//----------------------------------------------------------------------------------------------------------------------

class FDBPurge : public FDBTool {

public: // methods

    FDBPurge(int argc, char **argv) : FDBTool(argc, argv) {

        options_.push_back(new SimpleOption<bool>("doit", "Delete the files (data and indexes)"));

    }

private: // methods

    virtual void run();

    static void usage(const std::string &tool);

};

void FDBPurge::usage(const std::string &tool) {

    eckit::Log::info() << std::endl << "Usage: " << tool << " [--doit] [path1|request1] [path2|request2] ..." << std::endl;
    FDBTool::usage(tool);
}

void FDBPurge::run() {

    eckit::option::CmdArgs args(&FDBPurge::usage, -1, options_);
    bool doit = false;
    args.get("doit", doit);

    for (size_t i = 0; i < args.count(); ++i) {

        eckit::PathName path(databasePath(args.args(i)));

        Log::info() << "Scanning " << path << std::endl;

        fdb5::TocHandler handler(path);
        Log::info() << "Database key " << handler.databaseKey() << std::endl;

        std::vector<Index *> indexes = handler.loadIndexes();

        PurgeVisitor visitor(path);

        for (std::vector<Index *>::const_iterator i = indexes.begin(); i != indexes.end(); ++i) {
            (*i)->entries(visitor);
        }

        visitor.report(Log::info());

        bool doit = false;
        args.get("doit", doit);

        if (doit)
        {visitor.purge();}

        handler.freeIndexes(indexes);
    }

    if(!doit) {
        Log::info() << std::endl << "Rerun command with --doit flag to delete unused files" << std::endl;
    }
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBPurge app(argc, argv);
    return app.start();
}

/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/runtime/Tool.h"
#include "eckit/runtime/Context.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/Option.h"
#include "eckit/option/CmdArgs.h"

#include "fdb5/Index.h"
#include "fdb5/Key.h"
#include "fdb5/PurgeVisitor.h"
#include "fdb5/TocHandler.h"

using namespace std;
using namespace eckit;
using namespace eckit::option;
using namespace fdb5;

//----------------------------------------------------------------------------------------------------------------------

class FDBTool : public eckit::Tool {

  public: // methods

    FDBTool(int argc, char **argv) : eckit::Tool(argc, argv) {

        options_.push_back(new SimpleOption<std::string>("class", "keyword class"));
        options_.push_back(new SimpleOption<std::string>("stream", "keyword stream"));
        options_.push_back(new SimpleOption<std::string>("expver", "keyword expver"));
        options_.push_back(new SimpleOption<std::string>("date", "keyword class"));
        options_.push_back(new SimpleOption<std::string>("time", "keyword class"));
        options_.push_back(new SimpleOption<std::string>("domain", "keyword class"));

    }

  protected: // methods

    static void usage(const std::string &tool) {
    }

  protected: // members

    std::vector<Option *> options_;

};

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

    eckit::Log::info() << std::endl << "Usage: " << tool << " [--doit] [path1] [path2] ..." << std::endl;
    FDBTool::usage(tool);
}

void FDBPurge::run() {
    eckit::option::CmdArgs args(&FDBPurge::usage, -1, options_);
    bool doit = false;
    args.get("doit", doit);

    for (size_t i = 0; i < args.count(); ++i) {

        eckit::PathName path(args.args(i));

        if (!path.isDir()) {
            path = path.dirName();
        }

        path = path.realName();

        Log::info() << "Scanning " << path << std::endl;

        fdb5::TocHandler handler(path);

        std::vector<Index *> indexes = handler.loadIndexes();

        PurgeVisitor visitor(path);

        for (std::vector<Index *>::const_iterator i = indexes.begin(); i != indexes.end(); ++i) {
            (*i)->entries(visitor);
        }

        visitor.report(Log::info());

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

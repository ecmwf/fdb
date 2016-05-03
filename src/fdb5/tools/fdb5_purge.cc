/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/option/SimpleOption.h"
#include "eckit/option/CmdArgs.h"

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

    FDBPurge(int argc, char **argv) : FDBTool(argc, argv), doit_(false) {

        options_.push_back(new SimpleOption<bool>("doit", "Delete the files (data and indexes)"));

    }

  private: // methods

    virtual void process(const eckit::PathName&, const eckit::option::CmdArgs& args);
    virtual void usage(const std::string &tool);
    virtual void init(const eckit::option::CmdArgs& args);
    virtual void finish(const eckit::option::CmdArgs& args);

    bool doit_;
};

void FDBPurge::usage(const std::string &tool) {

    eckit::Log::info() << std::endl << "Usage: " << tool << " [--doit] [path1|request1] [path2|request2] ..." << std::endl;
    FDBTool::usage(tool);
}

void FDBPurge::init(const eckit::option::CmdArgs& args) {
    args.get("doit", doit_);
}

void FDBPurge::process(const eckit::PathName& path, const eckit::option::CmdArgs& args) {

    Log::info() << "Scanning " << path << std::endl;

    fdb5::TocHandler handler(path);
    Log::info() << "Database key " << handler.databaseKey() << std::endl;

    std::vector<Index *> indexes = handler.loadIndexes();

    PurgeVisitor visitor(path);

    for (std::vector<Index *>::const_iterator i = indexes.begin(); i != indexes.end(); ++i) {
        (*i)->entries(visitor);
    }

    visitor.report(Log::info());

    if (doit_) {
        visitor.purge();
    }

    handler.freeIndexes(indexes);
}


void FDBPurge::finish(const eckit::option::CmdArgs& args) {
    if (!doit_) {
        Log::info() << std::endl << "Rerun command with --doit flag to delete unused files" << std::endl;
    }
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBPurge app(argc, argv);
    return app.start();
}

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/tools/FDBTool.h"

#include "eckit/option/CmdArgs.h"
#include "eckit/config/LocalConfiguration.h"

using namespace eckit;

//----------------------------------------------------------------------------------------------------------------------

class FDBReconsolidateToc : public fdb5::FDBTool {

  public: // methods

    FDBReconsolidateToc(int argc, char **argv) :
        fdb5::FDBTool(argc, argv) {}

  private: // methods

    virtual void usage(const std::string &tool) const;
    virtual void execute(const eckit::option::CmdArgs& args);
};

void FDBReconsolidateToc::usage(const std::string &tool) const {
    Log::info() << std::endl
                << "Usage: " << tool << " path" << std::endl;
    fdb5::FDBTool::usage(tool);
}


void FDBReconsolidateToc::execute(const eckit::option::CmdArgs& args) {

    if (args.count() != 1) {
        usage(args.tool());
        exit(1);
    }

    // We want the directory associated with the

    eckit::PathName dbPath(args(0));

    if (!dbPath.isDir()) {
        ASSERT(dbPath.baseName() == "toc");
        dbPath = dbPath.dirName();
    }

    // TODO: In updated version, grab default Config() here;

    //fdb5::TocDBWriter writer(dbPath, eckit::LocalConfiguration());
    //writer.reconsolidateIndexesAndTocs();

    std::unique_ptr<fdb5::DB> db = fdb5::DB::buildWriter(eckit::URI("toc", dbPath), eckit::LocalConfiguration());
    db->reconsolidateIndexesAndTocs();
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBReconsolidateToc app(argc, argv);
    return app.start();
}


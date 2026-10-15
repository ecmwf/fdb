// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/database/Catalogue.h"
#include "fdb5/tools/FDBTool.h"

#include "eckit/config/LocalConfiguration.h"
#include "eckit/filesystem/URI.h"
#include "eckit/option/CmdArgs.h"

using namespace eckit;

//----------------------------------------------------------------------------------------------------------------------

class FDBReconsolidateToc : public fdb5::FDBTool {

public:  // methods

    FDBReconsolidateToc(int argc, char** argv) : fdb5::FDBTool(argc, argv) {}

private:  // methods

    virtual void usage(const std::string& tool) const;
    virtual void execute(const eckit::option::CmdArgs& args);
};

void FDBReconsolidateToc::usage(const std::string& tool) const {
    Log::info() << std::endl << "Usage: " << tool << " path" << std::endl;
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

    std::unique_ptr<fdb5::CatalogueWriter> catalogue =
        fdb5::CatalogueWriterFactory::instance().build(eckit::URI("toc", dbPath), config(args));

    catalogue->reconsolidate();
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char** argv) {
    FDBReconsolidateToc app(argc, argv);
    return app.start();
}

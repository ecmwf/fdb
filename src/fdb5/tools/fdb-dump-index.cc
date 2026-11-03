// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "eckit/option/CmdArgs.h"

#include "fdb5/toc/TocHandler.h"
#include "fdb5/tools/FDBTool.h"

#include <string>

using namespace eckit;

//----------------------------------------------------------------------------------------------------------------------

class FDBDumpIndex : public fdb5::FDBTool {

public:  // methods

    FDBDumpIndex(int argc, char** argv) : fdb5::FDBTool(argc, argv) {}

private:  // methods

    virtual void usage(const std::string& tool) const;
    virtual void execute(const option::CmdArgs& args);
};

void FDBDumpIndex::usage(const std::string& tool) const {
    Log::info() << std::endl << "Usage: " << tool << " [path1] [path2] ..." << std::endl;
    fdb5::FDBTool::usage(tool);
}


void FDBDumpIndex::execute(const option::CmdArgs& args) {

    // n.b. We don't just open the toc, then check if in the list of indexes, as there
    //      is no reason to think that the indexes map to the same toc (or directory).

    for (size_t i = 0; i < args.count(); i++) {

        PathName idxPath(args(i));
        Log::info() << "Dumping contents of index file " << idxPath << std::endl;

        fdb5::TocHandler toc(idxPath.dirName(), fdb5::Config().expandConfig());

        toc.dumpIndexFile(Log::info(), idxPath);
    }
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char** argv) {
    FDBDumpIndex app(argc, argv);
    return app.start();
}

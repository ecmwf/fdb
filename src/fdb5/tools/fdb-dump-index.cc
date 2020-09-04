/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/option/CmdArgs.h"

#include "fdb5/toc/TocHandler.h"
#include "fdb5/toc/TocIndex.h"
#include "fdb5/tools/FDBTool.h"

#include "fdb5/toc/BTreeIndex.h"
#include "fdb5/database/Index.h"
//#include "fdb5/database/Field.h"
#include <string>

using namespace eckit;

//----------------------------------------------------------------------------------------------------------------------

class FDBDumpToc : public fdb5::FDBTool {

  public: // methods

    FDBDumpToc(int argc, char **argv) :
        fdb5::FDBTool(argc, argv) {}

  private: // methods

    virtual void usage(const std::string &tool) const;
    virtual void execute(const option::CmdArgs& args);
};

void FDBDumpToc::usage(const std::string &tool) const {
    Log::info() << std::endl
                << "Usage: " << tool << " [path1] [path2] ..." << std::endl;
    fdb5::FDBTool::usage(tool);
}


void FDBDumpToc::execute(const option::CmdArgs& args) {

    // n.b. We don't just open the toc, then check if in the list of indexes, as there
    //      is no reason to think that the indexes map to the same toc (or directory).

    for (size_t i = 0; i < args.count(); i++) {

        PathName idxPath(args(i));
        Log::info() << "Dumping contents of index file " << idxPath << std::endl;

        fdb5::TocHandler toc(idxPath.dirName());

        toc.dumpIndexFile(Log::info(), idxPath);
    }
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBDumpToc app(argc, argv);
    return app.start();
}


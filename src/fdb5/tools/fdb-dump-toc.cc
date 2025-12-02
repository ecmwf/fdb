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
#include "eckit/option/SimpleOption.h"

#include "fdb5/toc/TocHandler.h"
#include "fdb5/tools/FDBTool.h"

using namespace eckit;

//----------------------------------------------------------------------------------------------------------------------

class FDBDumpToc : public fdb5::FDBTool {

public:  // methods

    FDBDumpToc(int argc, char** argv) : fdb5::FDBTool(argc, argv) {
        // FDBDumpToc does not require to read the configuration
        needsConfig_ = false;
        options_.push_back(
            new eckit::option::SimpleOption<bool>("walk", "Walk subtocs rather than show simple entries"));
        options_.push_back(
            new eckit::option::SimpleOption<bool>("structure", "Add toc record lengths and offsets to output"));
    }

private:  // methods

    virtual void usage(const std::string& tool) const;
    virtual void execute(const eckit::option::CmdArgs& args);
};

void FDBDumpToc::usage(const std::string& tool) const {
    Log::info() << std::endl << "Usage: " << tool << " [path1] [path2] ..." << std::endl;
    fdb5::FDBTool::usage(tool);
}


void FDBDumpToc::execute(const eckit::option::CmdArgs& args) {

    bool walkSubTocs = args.getBool("walk", false);
    bool dumpStructure = args.getBool("structure", false);

    for (size_t i = 0; i < args.count(); i++) {

        eckit::PathName path(args(i));

        fdb5::TocHandler handler(path, fdb5::Key{});

        handler.dump(eckit::Log::info(), true, walkSubTocs, dumpStructure);
    }
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char** argv) {
    FDBDumpToc app(argc, argv);
    return app.start();
}

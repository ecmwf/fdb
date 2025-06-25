/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <memory>

#include "eckit/log/Log.h"
#include "eckit/option/CmdArgs.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/config/Config.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/tools/FDBTool.h"

using namespace eckit;

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class FdbSchema : public FDBTool {
    virtual void execute(const eckit::option::CmdArgs& args);
    virtual void usage(const std::string& tool) const;

public:

    FdbSchema(int argc, char** argv) : FDBTool(argc, argv) {}
};

void FdbSchema::usage(const std::string& tool) const {
    eckit::Log::info() << std::endl << "Usage: " << tool << " [shema] ..." << std::endl;
    FDBTool::usage(tool);
}

void FdbSchema::execute(const eckit::option::CmdArgs& args) {


    // With no arguments, provide the current master configuration schema (i.e. that selected by FDB_HOME)

    if (args.count() == 0 || args.has("config")) {
        Config conf = config(args);
        conf.schema().dump(Log::info());
    }
    else {

        // If the argument specifies a schema file, then examine that. Otherwise load the DB which is
        // pointed to, and return the schema presented by that.

        for (size_t i = 0; i < args.count(); i++) {

            eckit::PathName path(args(i));

            if (path.isDir()) {
                std::unique_ptr<DB> db = DB::buildReader(eckit::URI("toc", path));
                ASSERT(db->open());
                db->schema().dump(Log::info());
            }
            else {
                Schema schema;
                schema.load(args(i));
                schema.dump(Log::info());
            }
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace tools
}  // namespace fdb5

int main(int argc, char** argv) {
    fdb5::tools::FdbSchema app(argc, argv);
    return app.start();
}

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/option/CmdArgs.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/tools/FDBTool.h"

#include <memory>

namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------

class FdbRoot: public FDBTool {
public:  // methods
    FdbRoot(int argc, char** argv): FDBTool(argc, argv) {
        options_.push_back(
            new eckit::option::SimpleOption<bool>("create", "If a DB does not exist for the provided key, create it"));
    }

private:  // methods
    virtual void execute(const eckit::option::CmdArgs& args);
    virtual void usage(const std::string& tool) const;
};

void FdbRoot::usage(const std::string& tool) const {

    eckit::Log::info() << "\nUsage: " << tool << " [options] [request1] [request2] ...\n\n\n"
                       << "Examples:\n=========\n\n"
                       << tool << " class=od,expver=0001,stream=oper,date=20160428,time=1200\n\n";

    FDBTool::usage(tool);
}

void FdbRoot::execute(const eckit::option::CmdArgs& args) {

    const auto conf = config(args);

    const bool create = args.getBool("create", false);

    for (size_t i = 0; i < args.count(); ++i) {

        // domain add here as default
        auto parsed = FDBToolRequest::requestsFromString("domain=g," + args(i), {}, false, "read");

        for (const auto& request : parsed) {

            const auto& keys = conf.schema().expandDatabase(request.request());

            if (keys.empty()) { throw eckit::UserError("Invalid request", Here()); }

            for (const auto& key : keys) {

                eckit::Log::info() << key << std::endl;

                // 'Touch' the database (which will create it if it doesn't exist)
                std::unique_ptr<DB> db = DB::buildReader(key, conf);

                if (!db->exists() && create) { db = DB::buildWriter(key, conf); }

                if (db->exists()) { eckit::Log::info() << (*db) << std::endl; }
            }
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char **argv) {
    fdb5::tools::FdbRoot app(argc, argv);
    return app.start();
}

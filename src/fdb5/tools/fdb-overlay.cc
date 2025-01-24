/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/LibFdb5.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Key.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/tools/FDBTool.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/option/VectorOption.h"
#include "eckit/types/Types.h"

#include <sstream>
#include <string>
#include <vector>

using namespace eckit;
using namespace eckit::option;

namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------

class FdbOverlay : public FDBTool {

public:  // methods
    FdbOverlay(int argc, char** argv) : FDBTool(argc, argv) {
        options_.push_back(new VectorOption<std::string>("variable-keys",
                                                         "The keywords that should vary between mounted DBs", 0, ","));
        options_.push_back(new SimpleOption<bool>("remove", "Remove/unmount existing FDB overlay"));
        options_.push_back(new SimpleOption<bool>("force", "Apply overlay even if target DB already exists"));
    }

private:  // methods
    void init(const option::CmdArgs& args) override;
    void execute(const option::CmdArgs& args) override;
    void usage(const std::string& tool) const override;

private:  // members
    eckit::StringSet variableKeys_ {"class", "expver"};

    bool remove_ {false};
    bool force_ {false};
};

void FdbOverlay::usage(const std::string& tool) const {

    Log::info() << std::endl
                << "Usage: " << tool << " [options] [source DB request] [target DB request]" << std::endl
                << std::endl
                << std::endl;
    FDBTool::usage(tool);
}

void FdbOverlay::init(const option::CmdArgs& args) {
    FDBTool::init(args);
    {
        eckit::StringList keys;
        args.get("variable-keys", keys);
        variableKeys_ = {keys.begin(), keys.end()};
    }
    remove_ = args.getBool("remove", remove_);
    force_  = args.getBool("force", force_);
}

void FdbOverlay::execute(const option::CmdArgs& args) {

    if (args.count() != 2) {
        usage("fdb-overlay");
        return;
    }

    auto parsedSource = FDBToolRequest::requestsFromString("domain=g," + args(0), {}, false, "read");
    auto parsedTarget = FDBToolRequest::requestsFromString("domain=g," + args(1), {}, false, "read");
    ASSERT(parsedSource.size() == 1);
    ASSERT(parsedTarget.size() == 1);

    const auto& sourceRequest = parsedSource.front();
    const auto& targetRequest = parsedTarget.front();
    ASSERT(!sourceRequest.all());
    ASSERT(!targetRequest.all());

    ASSERT(sourceRequest.request().count() == targetRequest.request().count());

    //------------------------------------------------------------------------------------------------------------------

    FDB fdb(config(args));

    const auto& conf = fdb.config();

    const auto& schema = conf.schema();

    TypedKey srcKey {schema.registry()};
    TypedKey tgtKey {schema.registry()};

    ASSERT(schema.expandFirstLevel(sourceRequest.request(), srcKey));
    ASSERT(schema.expandFirstLevel(targetRequest.request(), tgtKey));

    if (remove_) {
        Log::info() << "Removing " << srcKey << " from " << tgtKey << std::endl;
    } else {
        Log::info() << "Applying " << srcKey << " onto " << tgtKey << std::endl;
    }

    //------------------------------------------------------------------------------------------------------------------

    {
        auto dbTarget = CatalogueReaderFactory::instance().build(tgtKey.canonical(), conf);

        const auto& exists = dbTarget->exists();

        if (remove_) {
            if (!exists) {
                std::ostringstream oss;
                oss << "The '--remove' option expects an existing target database! Key: " << tgtKey << std::endl;
                throw eckit::UserError(oss.str(), Here());
            }
        } else {
            if (exists && !force_) {
                std::ostringstream oss;
                oss << "The target database already exists! Key: " << tgtKey << std::endl;
                eckit::Log::error() << oss.str() << std::endl;
                eckit::Log::error() << "To overlay an existing target database, re-run with `--force`" << std::endl;
                throw eckit::UserError(oss.str(), Here());
            }
        }
    }

    auto dbTarget = CatalogueWriterFactory::instance().build(tgtKey.canonical(), conf);
    auto dbSource = CatalogueReaderFactory::instance().build(srcKey.canonical(), conf);

    dbTarget->overlayDB(dbSource.get(), variableKeys_, remove_);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char** argv) {
    fdb5::tools::FdbOverlay app(argc, argv);
    return app.start();
}

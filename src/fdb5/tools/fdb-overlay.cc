/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/VectorOption.h"

#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"
#include "fdb5/config/UMask.h"
#include "fdb5/database/Key.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/toc/TocDBWriter.h"
#include "fdb5/toc/TocDBReader.h"
#include "fdb5/toc/TocEngine.h"
#include "fdb5/tools/FDBTool.h"

using namespace eckit;
using namespace eckit::option;

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class FdbOverlay : public FDBTool {

public: // methods

    FdbOverlay(int argc, char **argv) :
        FDBTool(argc, argv),
        variableKeys_{"class", "expver"},
        remove_(false),
        force_(false) {
        options_.push_back(new VectorOption<std::string>("variable-keys",
                                                         "The keys that may vary between mounted DBs",
                                                         0, ","));
        options_.push_back(new SimpleOption<bool>("remove", "Remove a previously FDB overlay"));
        options_.push_back(new SimpleOption<bool>("force", "Apply overlay even if target already exists"));
    }

private: // methods

    virtual void init(const option::CmdArgs& args);
    virtual void execute(const option::CmdArgs& args);
    virtual void usage(const std::string &tool) const;

private: // members

    std::vector<std::string> variableKeys_;
    bool remove_;
    bool force_;
};

void FdbOverlay::usage(const std::string &tool) const {

    Log::info() << std::endl
                << "Usage: " << tool << " [options] [source DB request] [target DB request]" << std::endl
                << std::endl
                << std::endl;
    FDBTool::usage(tool);
}

void FdbOverlay::init(const option::CmdArgs& args) {
    FDBTool::init(args);
    args.get("variable-keys", variableKeys_);
    args.get("remove", remove_);
    args.get("force", force_);
}

void FdbOverlay::execute(const option::CmdArgs& args) {

    UMask umask(UMask::defaultUMask());

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

    const Config& config = LibFdb5::instance().defaultConfig();
    const Schema& schema = config.schema();

    Key source;
    Key target;
    ASSERT(schema.expandFirstLevel(sourceRequest.request(), source));
    ASSERT(schema.expandFirstLevel(targetRequest.request(), target));

    if (remove_) {
        Log::info() << "Removing " << source << " from " << target << std::endl;
    } else {
        Log::info() << "Applying " << source << " onto " << target << std::endl;
    }

    std::unique_ptr<DB> dbSource(DBFactory::buildReader(source, config));
    if (!dbSource->exists()) {
        std::stringstream ss;
        ss << "Source database not found: " << source << std::endl;
        throw UserError(ss.str(), Here());
    }

    if (dbSource->dbType() != TocEngine::typeName()) {
        std::stringstream ss;
        ss << "Only TOC DBs currently supported" << std::endl;
        throw UserError(ss.str(), Here());
    }

    std::unique_ptr<DB> dbTarget(DBFactory::buildReader(target, config));

    if (remove_) {
        if (!dbTarget->exists()) {
            std::stringstream ss;
            ss << "Target database must already already exist: " << target << std::endl;
            throw UserError(ss.str(), Here());
        }
    } else {
        if (dbTarget->exists() && !force_) {
            std::stringstream ss;
            ss << "Target database already exists: " << target << std::endl;
            eckit::Log::error() << ss.str() << std::endl;
            eckit::Log::error() << "To mount to existing target, rerun with --force" << std::endl;
            throw UserError(ss.str(), Here());
        }
    }

    ASSERT(dbTarget->basePath() != dbSource->basePath());

    std::unique_ptr<DB> newDB(DBFactory::buildWriter(target, config));

    // This only works for tocDBs

    TocDBReader* tocSourceDB = dynamic_cast<TocDBReader*>(dbSource.get());
    TocDBWriter* tocTargetDB = dynamic_cast<TocDBWriter*>(newDB.get());
    ASSERT(tocSourceDB);
    ASSERT(tocTargetDB);

    std::set<std::string> vkeys(variableKeys_.begin(), variableKeys_.end());
    tocTargetDB->overlayDB(*tocSourceDB, vkeys, remove_);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fbb5

int main(int argc, char **argv) {
    fdb5::tools::FdbOverlay app(argc, argv);
    return app.start();
}

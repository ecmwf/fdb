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
#include "fdb5/database/Key.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/rules/Schema.h"
/*#include "fdb5/toc/TocCatalogueWriter.h"
#include "fdb5/toc/TocDBReader.h"*/
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
    remove_ = args.getBool("remove", false);
    force_ = args.getBool("force", false);
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

    Config conf = config(args);
    const Schema& schema = conf.schema();

    TypedKey source{conf.schema().registry()};
    TypedKey target{conf.schema().registry()};
    ASSERT(schema.expandFirstLevel(sourceRequest.request(), source));
    ASSERT(schema.expandFirstLevel(targetRequest.request(), target));

    if (remove_) {
        Log::info() << "Removing " << source << " from " << target << std::endl;
    } else {
        Log::info() << "Applying " << source << " onto " << target << std::endl;
    }

    if (source.keys() != target.keys()) {
        std::stringstream ss;
        ss << "Keys insufficiently matching for mount: " << source << " : " << target << std::endl;
        throw UserError(ss.str(), Here());
    }

    std::set<std::string> vkeys(variableKeys_.begin(), variableKeys_.end());
    for (const auto& kv : target) {
        auto it = source.find(kv.first);
        ASSERT(it != source.end());
        if (kv.second != it->second && vkeys.find(kv.first) == vkeys.end()) {
            std::stringstream ss;
            ss << "Key " << kv.first << " not allowed to differ between DBs: " << source << " : " << target;
            throw UserError(ss.str(), Here());
        }
    }

    std::unique_ptr<DB> dbSource = DB::buildReader(source.canonical(), conf);
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

    std::unique_ptr<DB> dbTarget = DB::buildReader(target.canonical(), conf);

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

    ASSERT(dbTarget->uri() != dbSource->uri());

    std::unique_ptr<DB> newDB = DB::buildWriter(target.canonical(), conf);

    // This only works for tocDBs

/*    TocDBReader* tocSourceDB = dynamic_cast<TocDBReader*>(dbSource.get());
    TocDBWriter* tocTargetDB = dynamic_cast<TocDBWriter*>(newDB.get());
    ASSERT(tocSourceDB);
    ASSERT(tocTargetDB);*/

    newDB->overlayDB(*dbSource, vkeys, remove_);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fbb5

int main(int argc, char **argv) {
    fdb5::tools::FdbOverlay app(argc, argv);
    return app.start();
}

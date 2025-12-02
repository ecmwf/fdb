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
#include "eckit/option/SimpleOption.h"
#include "eckit/option/VectorOption.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/toc/TocEngine.h"
#include "fdb5/tools/FDBTool.h"

#include <vector>

using namespace eckit::option;

namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------

class FdbOverlay : public FDBTool {

public:  // methods

    FdbOverlay(int argc, char** argv) : FDBTool(argc, argv), variableKeys_{"class", "expver"} {
        options_.push_back(
            new VectorOption<std::string>("variable-keys", "The keys that may vary between mounted DBs", 0, ","));
        options_.push_back(new SimpleOption<bool>("remove", "Remove a previously FDB overlay"));
        options_.push_back(new SimpleOption<bool>("force", "Apply overlay even if target already exists"));
    }

private:  // methods

    virtual void init(const CmdArgs& args);
    virtual void execute(const CmdArgs& args);
    virtual void usage(const std::string& tool) const;

private:  // members

    std::vector<std::string> variableKeys_;

    bool remove_{false};
    bool force_{false};
};

void FdbOverlay::usage(const std::string& tool) const {

    eckit::Log::info() << "\nUsage: " << tool << " [options] [source DB request] [target DB request]\n\n\n";

    FDBTool::usage(tool);
}

void FdbOverlay::init(const CmdArgs& args) {
    FDBTool::init(args);
    args.get("variable-keys", variableKeys_);
    remove_ = args.getBool("remove", remove_);
    force_ = args.getBool("force", force_);
}

void FdbOverlay::execute(const CmdArgs& args) {

    const Config conf = config(args);

    if (args.count() != 2) {
        usage("fdb-overlay");
        return;
    }

    bool injectDomain = false;
    std::vector<Key> sources = parse(args(0), conf);
    ASSERT(!sources.empty());

    const auto targets = parse(args(1), conf);
    ASSERT(!targets.empty());

    const auto& source = sources.front();
    const auto& target = targets.front();

    if (remove_) {
        eckit::Log::info() << "Removing " << source << " from " << target << std::endl;
    }
    else {
        eckit::Log::info() << "Applying " << source << " onto " << target << std::endl;
    }

    if (source.keys() != target.keys()) {
        std::stringstream ss;
        ss << "Keys insufficiently matching for mount: " << source << " : " << target << std::endl;
        throw eckit::UserError(ss.str(), Here());
    }

    std::set<std::string> vkeys(variableKeys_.begin(), variableKeys_.end());
    for (const auto& kv : target) {
        const auto [it, found] = source.find(kv.first);
        ASSERT(found);
        if (kv.second != it->second && vkeys.find(kv.first) == vkeys.end()) {
            std::stringstream ss;
            ss << "Key " << kv.first << " not allowed to differ between DBs: " << source << " : " << target;
            throw eckit::UserError(ss.str(), Here());
        }
    }

    std::unique_ptr<CatalogueReader> dbSource = CatalogueReaderFactory::instance().build(source, conf);
    if (!dbSource->exists()) {
        std::stringstream ss;
        ss << "Source database not found: " << source << std::endl;
        throw eckit::UserError(ss.str(), Here());
    }

    if (dbSource->type() != TocEngine::typeName()) {
        std::stringstream ss;
        ss << "Only TOC DBs currently supported" << std::endl;
        throw eckit::UserError(ss.str(), Here());
    }

    std::unique_ptr<CatalogueReader> dbTarget = CatalogueReaderFactory::instance().build(target, conf);

    if (remove_) {
        if (!dbTarget->exists()) {
            std::stringstream ss;
            ss << "Target database must already exist: " << target << std::endl;
            throw eckit::UserError(ss.str(), Here());
        }
    }
    else {
        if (dbTarget->exists() && !force_) {
            std::stringstream ss;
            ss << "Target database already exists: " << target << std::endl;
            eckit::Log::error() << ss.str() << std::endl;
            eckit::Log::error() << "To mount to existing target, rerun with --force" << std::endl;
            throw eckit::UserError(ss.str(), Here());
        }
    }

    ASSERT(dbTarget->uri() != dbSource->uri());

    auto newCatalogue = CatalogueWriterFactory::instance().build(target, conf);
    if (newCatalogue->type() == TocEngine::typeName() && dbSource->type() == TocEngine::typeName()) {
        newCatalogue->overlayDB(*dbSource, vkeys, remove_);
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char** argv) {
    fdb5::tools::FdbOverlay app(argc, argv);
    return app.start();
}

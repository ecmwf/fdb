// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "eckit/exception/Exceptions.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/toc/TocCatalogueWriter.h"
#include "fdb5/toc/TocEngine.h"
#include "fdb5/tools/FDBTool.h"

using namespace eckit::option;

namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------

class FdbHide : public FDBTool {
public:  // methods

    FdbHide(int argc, char** argv) : FDBTool(argc, argv) {
        options_.push_back(new SimpleOption<bool>("doit", "Do the actual change"));
    }

private:  // methods

    void init(const CmdArgs& args) override;
    void execute(const CmdArgs& args) override;
    void usage(const std::string& tool) const override;

private:  // members

    bool doit_{false};
};

void FdbHide::usage(const std::string& tool) const {

    eckit::Log::info() << "\nUsage: " << tool << " [options] [DB request]\n\n\n";

    FDBTool::usage(tool);
}

void FdbHide::init(const CmdArgs& args) {
    FDBTool::init(args);
    doit_ = args.getBool("doit", doit_);
}

void FdbHide::execute(const CmdArgs& args) {

    Config conf = config(args);

    if (args.count() != 1) {
        usage("fdb-hide");
        return;
    }

    std::vector<Key> keys = parse(args(0), conf);

    if (keys.empty()) {
        throw eckit::UserError("Invalid request", Here());
    }

    /// @todo do we want to assert that expandDatabase returns only one key ?

    for (const auto& key : keys) {

        auto db = CatalogueReaderFactory::instance().build(key, conf);
        if (!db->exists()) {
            std::ostringstream ss;
            ss << "Database not found: " << key << std::endl;
            throw eckit::UserError(ss.str(), Here());
        }

        if (db->type() != TocEngine::typeName()) {
            throw eckit::UserError("Only TOC DBs currently supported", Here());
        }

        eckit::Log::info() << "Hide contents of DB: " << *db << std::endl;
        if (doit_) {
            auto dbWriter = CatalogueWriterFactory::instance().build(key, conf);
            auto* tocDB = dynamic_cast<TocCatalogueWriter*>(dbWriter.get());
            tocDB->hideContents();
        }
        else {
            eckit::Log::info() << "Run with --doit to make changes" << std::endl;
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char** argv) {
    fdb5::tools::FdbHide app(argc, argv);
    return app.start();
}

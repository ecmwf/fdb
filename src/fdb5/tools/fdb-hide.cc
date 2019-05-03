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

class FdbHide : public FDBTool {

public: // methods

    FdbHide(int argc, char **argv) :
        FDBTool(argc, argv),
        doit_(false) {
        options_.push_back(new SimpleOption<bool>("doit", "Do the actual change"));
    }

private: // methods

    virtual void init(const option::CmdArgs& args);
    virtual void execute(const option::CmdArgs& args);
    virtual void usage(const std::string &tool) const;

private: // members

    bool doit_;
};

void FdbHide::usage(const std::string &tool) const {

    Log::info() << std::endl
                << "Usage: " << tool << " [options] [source DB request] [target DB request] ..." << std::endl
                << std::endl
                << std::endl;
    FDBTool::usage(tool);
}

void FdbHide::init(const option::CmdArgs& args) {
    FDBTool::init(args);
    args.get("doit", doit_);
}

void FdbHide::execute(const option::CmdArgs& args) {

    UMask umask(UMask::defaultUMask());

    if (args.count() != 1) {
        usage("fdb-hide");
        return;
    }

    auto dbrequests = FDBToolRequest::requestsFromString("domain=g," + args(0), {}, false, "read");
    ASSERT(dbrequests.size() == 1);

    const auto& dbrequest = dbrequests.front();
    ASSERT(!dbrequest.all());

    const Config& config = LibFdb5::instance().defaultConfig();
    const Schema& schema = config.schema();

    Key dbkey;
    ASSERT(schema.expandFirstLevel(dbrequest.request(), dbkey));

    std::unique_ptr<DB> db(DBFactory::buildReader(dbkey, config));
    if (!db->exists()) {
        std::stringstream ss;
        ss << "Database not found: " << dbkey << std::endl;
        throw UserError(ss.str(), Here());
    }

    if (db->dbType() != TocEngine::typeName()) {
        std::stringstream ss;
        ss << "Only TOC DBs currently supported" << std::endl;
        throw UserError(ss.str(), Here());
    }

    eckit::Log::info() << "Hide contents of DB: " << *db << std::endl;
    if (doit_) {
        std::unique_ptr<DB> dbWriter(DBFactory::buildWriter(dbkey, config));
        TocDBWriter* tocDB = dynamic_cast<TocDBWriter*>(dbWriter.get());
        tocDB->hideContents();
    } else {
        eckit::Log::info() << "Run with --doit to make changes" << std::endl;
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fbb5

int main(int argc, char **argv) {
    fdb5::tools::FdbHide app(argc, argv);
    return app.start();
}
/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"
#include "eckit/memory/ScopedPtr.h"
#include "eckit/option/CmdArgs.h"

#include "fdb5/LibFdb.h"
#include "fdb5/config/UMask.h"
#include "fdb5/database/Key.h"
#include "fdb5/rules/Schema.h"

#include "fdb5/tools/FDBTool.h"
#include "fdb5/tools/ToolRequest.h"

//----------------------------------------------------------------------------------------------------------------------

class FdbRoot : public fdb5::FDBTool {

public: // methods

    FdbRoot(int argc, char **argv) :
        fdb5::FDBTool(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<bool>("create", "If a DB does not exist for the provided key, create it"));
    }

private: // methods

    virtual void execute(const eckit::option::CmdArgs& args);
    virtual void usage(const std::string &tool) const;
};

void FdbRoot::usage(const std::string &tool) const {

    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " [options] [request1] [request2] ..." << std::endl
                       << std::endl
                       << std::endl
                       << "Examples:" << std::endl
                       << "=========" << std::endl << std::endl
                       << tool << " class=od,expver=0001,stream=oper,date=20160428,time=1200"
                       << std::endl
                       << std::endl;
    FDBTool::usage(tool);
}

void FdbRoot::execute(const eckit::option::CmdArgs& args) {

    bool create_db;
    args.get("create", create_db);

    fdb5::UMask umask(fdb5::UMask::defaultUMask());

    for (size_t i = 0; i < args.count(); ++i) {

        fdb5::ToolRequest req("domain=g," + args(i)); // domain add here as default

        const fdb5::Schema& schema = fdb5::LibFdb::instance().schema();
        fdb5::Key result;
        ASSERT( schema.expandFirstLevel(req.key(), result) );

        eckit::Log::info() << result << std::endl;

        // 'Touch' the database (which will create it if it doesn't exist)
        eckit::ScopedPtr<fdb5::DB> db(fdb5::DBFactory::buildReader(result));

        if (!db->exists() && create_db) {
            db.reset(fdb5::DBFactory::buildWriter(result));
        }

        if (db->exists()) {
            eckit::Log::info() << (*db) << std::endl;
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FdbRoot app(argc, argv);
    return app.start();
}

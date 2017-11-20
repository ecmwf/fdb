/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/memory/ScopedPtr.h"
#include "eckit/option/CmdArgs.h"

#include "fdb5/LibFdb.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/tools/FDBInspect.h"

#include "mars_server_config.h"
#include "mars_server_version.h"

using eckit::Log;

//----------------------------------------------------------------------------------------------------------------------

class FDBInfo : public fdb5::FDBTool {

  public: // methods

    FDBInfo(int argc, char **argv) :
        fdb5::FDBTool(argc, argv),
        all_(false),
        version_(false),
        home_(false),
        schema_(false)
    {
        options_.push_back(new eckit::option::SimpleOption<bool>("all", "Print all information"));
        options_.push_back(new eckit::option::SimpleOption<bool>("version", "Print the version of the FDB/Mars server being used"));
        options_.push_back(new eckit::option::SimpleOption<bool>("home", "Print the location of the FDB configuration files"));
        options_.push_back(new eckit::option::SimpleOption<bool>("schema", "Print the location of the FDB schema file"));
    }

  private: // methods

    virtual void usage(const std::string &tool) const;
    virtual void execute(const eckit::option::CmdArgs& args);
    virtual void init(const eckit::option::CmdArgs &args);

    bool all_;
    bool version_;
    bool home_;
    bool schema_;
};

void FDBInfo::usage(const std::string &tool) const {
    Log::info() << std::endl
                << "Usage: " << tool << " [options]" << std::endl
                << std::endl
                << std::endl
                << "Examples:" << std::endl
                << "=========" << std::endl << std::endl
                << tool << " --all" << std::endl
                << tool << " --version" << std::endl
                << tool << " --home" << std::endl
                << tool << " --schema" << std::endl
                << std::endl;
    FDBTool::usage(tool);
}

void FDBInfo::init(const eckit::option::CmdArgs &args) {
    args.get("all", all_);
    args.get("version", version_);
    args.get("home", home_);
    args.get("schema", schema_);
}

void FDBInfo::execute(const eckit::option::CmdArgs&) {

    if(all_ || version_) {
        Log::info() << (all_ ? "Version: " : "") << mars_server_version_str() << std::endl;
        if(!all_) return;
    }

    if(all_ || home_) {
        // print FDB_HOME and exit -- note that is used in the bin/fdb wrapper script
        Log::info() << (all_ ? "Home: " : "") << eckit::PathName("~fdb/") << std::endl;
        if(!all_) return;
    }

    if(all_ || schema_) {
        Log::info() << (all_ ? "Schema: " : "") << fdb5::LibFdb::instance().schemaPath() << std::endl;
        if(!all_) return;
    }
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBInfo app(argc, argv);
    return app.start();
}


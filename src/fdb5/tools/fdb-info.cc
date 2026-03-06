/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/config/Config.h"
#include "fdb5/fdb5_version.h"
#include "fdb5/io/LustreSettings.h"
#include "fdb5/tools/FDBTool.h"

using eckit::Log;

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBInfo : public FDBTool {

public:  // methods

    FDBInfo(int argc, char** argv) :
        FDBTool(argc, argv), all_(false), version_(false), home_(false), schema_(false), config_(false) {
        options_.push_back(new eckit::option::SimpleOption<bool>("all", "Print all information"));
        options_.push_back(new eckit::option::SimpleOption<bool>("version", "Print the version of the FDB being used"));
        options_.push_back(
            new eckit::option::SimpleOption<bool>("home", "Print the location of the FDB configuration files"));
        options_.push_back(
            new eckit::option::SimpleOption<bool>("schema", "Print the location of the FDB schema file"));
        options_.push_back(new eckit::option::SimpleOption<bool>(
            "config-file", "Print the location of the FDB configuration file if being used"));
        options_.push_back(new eckit::option::SimpleOption<bool>(
            "lustre-api", "Indicate if the Lustre API is supported or disabled in this build"));
    }

private:  // methods

    virtual void usage(const std::string& tool) const;
    virtual void execute(const eckit::option::CmdArgs& args);
    virtual void init(const eckit::option::CmdArgs& args);

    bool all_;
    bool version_;
    bool home_;
    bool schema_;
    bool config_;
    bool lustreApi_;
};

void FDBInfo::usage(const std::string& tool) const {
    Log::info() << std::endl
                << "Usage: " << tool << " [options]" << std::endl
                << std::endl
                << std::endl
                << "Examples:" << std::endl
                << "=========" << std::endl
                << std::endl
                << tool << " --all" << std::endl
                << tool << " --version" << std::endl
                << tool << " --home" << std::endl
                << tool << " --schema" << std::endl
                << tool << " --config-file" << std::endl
                << tool << " --lustre-api" << std::endl
                << std::endl;
    FDBTool::usage(tool);
}

void FDBInfo::init(const eckit::option::CmdArgs& args) {
    all_ = args.getBool("all", false);
    version_ = args.getBool("version", false);
    home_ = args.getBool("home", false);
    schema_ = args.getBool("schema", false);
    config_ = args.getBool("config-file", false);
    lustreApi_ = args.getBool("lustre-api", false);
}

void FDBInfo::execute(const eckit::option::CmdArgs& args) {

    if (all_ || version_) {
        Log::info() << (all_ ? "Version: " : "") << fdb5_version_str() << std::endl;
        if (!all_) {
            return;
        }
    }

    if (all_ || home_) {
        // print FDB_HOME and exit -- note that is used in the bin/fdb wrapper script
        Log::info() << (all_ ? "Home: " : "") << eckit::PathName("~fdb/") << std::endl;
        if (!all_) {
            return;
        }
    }

    Config conf = config(args);

    if (all_ || schema_) {
        Log::info() << (all_ ? "Schema: " : "") << conf.schemaPath() << std::endl;
        if (!all_) {
            return;
        }
    }

    if (all_ || config_) {
        Log::info() << (all_ ? "Config: " : "") << conf.configPath() << std::endl;
        if (!all_) {
            return;
        }
    }

    if (all_ || lustreApi_) {
        Log::info() << (all_ ? "Lustre-API: " : "") << (fdb5LustreapiSupported() ? "enabled" : "disabled") << std::endl;
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace tools
}  // namespace fdb5

int main(int argc, char** argv) {
    fdb5::tools::FDBInfo app(argc, argv);
    return app.start();
}

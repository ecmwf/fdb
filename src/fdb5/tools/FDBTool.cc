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
#include "fdb5/tools/FDBTool.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static FDBTool* instance_ = nullptr;

FDBTool::FDBTool(int argc, char** argv) : eckit::Tool(argc, argv, "FDB_HOME") {
    ASSERT(instance_ == nullptr);
    instance_ = this;
}

static void usage(const std::string& tool) {
    ASSERT(instance_);
    instance_->usage(tool);
}

void FDBTool::run() {
    options_.push_back(new eckit::option::SimpleOption<std::string>("config", "FDB configuration filename"));

    eckit::option::CmdArgs args(&fdb5::usage, options_, numberOfPositionalArguments(),
                                minimumPositionalArguments());


    init(args);
    execute(args);
    finish(args);
}

Config FDBTool::config(const eckit::option::CmdArgs& args, const eckit::Configuration& userConfig) const {

    if (args.has("config")) {
        std::string config = args.getString("config", "");
        if (config.empty()) {
            throw eckit::UserError("Missing config file name", Here());
        }
        eckit::PathName configPath(config);
        if (!configPath.exists()) {
            std::ostringstream ss;
            ss << "Path " << config << " does not exist";
            throw eckit::UserError(ss.str(), Here());
        }
        if (configPath.isDir()) {
            std::ostringstream ss;
            ss << "Path " << config << " is a directory. Expecting a file";
            throw eckit::UserError(ss.str(), Here());
        }
        return Config::make(configPath, userConfig);
    }

    return LibFdb5::instance().defaultConfig(userConfig);
}

void FDBTool::usage(const std::string&) const {}

void FDBTool::init(const eckit::option::CmdArgs&) {}

void FDBTool::finish(const eckit::option::CmdArgs&) {}

//----------------------------------------------------------------------------------------------------------------------

FDBToolException::FDBToolException(const std::string& w) : Exception(w) {}

FDBToolException::FDBToolException(const std::string& w, const eckit::CodeLocation& l) :
    Exception(w, l) {}


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

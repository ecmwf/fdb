/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/option/CmdArgs.h"
#include "eckit/types/Date.h"

#include "fdb5/config/MasterConfig.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/toc/TocDB.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/tools/FDBTool.h"

using eckit::Log;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static FDBTool* instance_ = 0;

FDBTool::FDBTool(int argc, char **argv):
    eckit::Tool(argc, argv, "FDB_HOME") {
    ASSERT(instance_ == 0);
    instance_ = this;

    MasterConfig::instance(); // ensure we initialize the configuration
}

static void usage(const std::string &tool) {
    ASSERT(instance_);
    instance_->usage(tool);
}

void FDBTool::run() {
    eckit::option::CmdArgs args(&fdb5::usage,
        options_,
        numberOfPositionalArguments(),
        minimumPositionalArguments());

    init(args);
    execute(args);
    finish(args);
}


void FDBTool::usage(const std::string &tool) const {

}

void FDBTool::init(const eckit::option::CmdArgs& args) {

}

void FDBTool::finish(const eckit::option::CmdArgs& args) {

}



//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5


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

#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/tools/FDBInspect.h"


using eckit::Log;

//----------------------------------------------------------------------------------------------------------------------

class FDBInfo : public fdb5::FDBTool {

  public: // methods

    FDBInfo(int argc, char **argv) :
        fdb5::FDBTool(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<bool>("home", "Print the location of the FDB configuration files. Can be overriden by FDB_HOME"));
    }

  private: // methods

    virtual void usage(const std::string &tool) const;
    virtual void execute(const eckit::option::CmdArgs& args);
    virtual void init(const eckit::option::CmdArgs &args);

    bool home_;
};

void FDBInfo::usage(const std::string &tool) const {
    Log::info() << std::endl
                << "Usage: " << tool << " [options]" << std::endl
                << std::endl
                << std::endl
                << "Examples:" << std::endl
                << "=========" << std::endl << std::endl
                << tool << " --home"
                << std::endl;
    FDBTool::usage(tool);
}

void FDBInfo::init(const eckit::option::CmdArgs &args) {
    args.get("home", home_);
}

void FDBInfo::execute(const eckit::option::CmdArgs&) {

    if(home_) {
        // print FDB_HOME and exit -- note that is used in the bin/fdb wrapper script
        Log::info() << eckit::PathName("~") << std::endl;
        return;
    }

}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBInfo app(argc, argv);
    return app.start();
}


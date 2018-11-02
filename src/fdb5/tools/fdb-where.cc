/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"
#include "eckit/option/CmdArgs.h"

#include "fdb5/database/Manager.h"
#include "fdb5/tools/FDBInspect.h"

using eckit::Log;

//----------------------------------------------------------------------------------------------------------------------

class FDBWhere : public fdb5::FDBInspect {
  public: // methods

    FDBWhere(int argc, char **argv) : fdb5::FDBInspect(argc, argv) {}

  private: // methods

    virtual void usage(const std::string &tool) const;
    virtual void process(const eckit::PathName&, const eckit::option::CmdArgs& args);

};

void FDBWhere::usage(const std::string &tool) const {

    Log::info() << std::endl
                << "Usage: " << tool << std::endl;
    FDBInspect::usage(tool);
}

void FDBWhere::process(const eckit::PathName& path, const eckit::option::CmdArgs&)  {
    Log::info() << path << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBWhere app(argc, argv);
    return app.start();
}

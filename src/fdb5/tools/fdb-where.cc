/*
 * (C) Copyright 1996-2013 ECMWF.
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
#include "fdb5/tools/ToolRequest.h"

using eckit::Log;

//----------------------------------------------------------------------------------------------------------------------

class FDBWhere : public fdb5::FDBInspect {
  public: // methods

    FDBWhere(int argc, char **argv) : fdb5::FDBInspect(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<std::string>("request", "Provide a request to match 'class:stream:expver', e.g. --request=class=od,expver=0001"));

    }

  private: // methods

    virtual void usage(const std::string &tool) const;
    virtual void process(const eckit::PathName&, const eckit::option::CmdArgs& args);
    virtual void finish(const eckit::option::CmdArgs& args);

};

void FDBWhere::usage(const std::string &tool) const {

    Log::info() << std::endl
                << "Usage: " << tool << std::endl
                << "       " << tool << "--request <request>" << std::endl
                << "       " << tool << "path1|request1 [path2|request2] ..." << std::endl;
    FDBInspect::usage(tool);
}

void FDBWhere::process(const eckit::PathName& path, const eckit::option::CmdArgs& args)  {
    Log::info() << path << std::endl;
}

void FDBWhere::finish(const eckit::option::CmdArgs& args) {

    if (args.count() == 0) {
        std::string s;
        args.get("request", s);

        fdb5::ToolRequest req(s);

        std::vector<eckit::PathName> roots = fdb5::Manager::visitableLocations(req.key());

        if(!roots.size()) {
            Log::info() << "No roots found for request with key " << req.key() << std::endl;
        }

        for (std::vector<eckit::PathName>::const_iterator i = roots.begin(); i != roots.end(); ++i) {
            Log::info() << *i << std::endl;
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBWhere app(argc, argv);
    return app.start();
}

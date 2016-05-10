/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/option/CmdArgs.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/tools/FDBInspect.h"

//----------------------------------------------------------------------------------------------------------------------

class FDBWipe : public fdb5::FDBInspect {

  public: // methods

    FDBWipe(int argc, char **argv) :
        fdb5::FDBInspect(argc, argv),
        doit_(false){

        options_.push_back(new eckit::option::SimpleOption<bool>("doit", "Delete the files (data and indexes)"));

    }

  private: // methods

    virtual void process(const eckit::PathName &, const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual void init(const eckit::option::CmdArgs &args);
    virtual void finish(const eckit::option::CmdArgs &args);

    bool doit_;

};

void FDBWipe::usage(const std::string &tool) const {

    eckit::Log::info() << std::endl << "Usage: " << tool << " [--doit] [path1|request1] [path2|request2] ..." << std::endl;
    FDBInspect::usage(tool);
}

void FDBWipe::init(const eckit::option::CmdArgs &args) {
    args.get("doit", doit_);
}

void FDBWipe::process(const eckit::PathName &path, const eckit::option::CmdArgs &args) {

    NOTIMP;
    eckit::Log::info() << "Scanning " << path << std::endl;

    fdb5::TocHandler handler(path);
    eckit::Log::info() << "Database key " << handler.databaseKey() << std::endl;

}


void FDBWipe::finish(const eckit::option::CmdArgs &args) {

}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBWipe app(argc, argv);
    return app.start();
}

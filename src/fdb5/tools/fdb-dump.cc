/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/memory/ScopedPtr.h"
#include "eckit/option/CmdArgs.h"

#include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/tools/FDBInspect.h"

//----------------------------------------------------------------------------------------------------------------------

class FDBDump : public fdb5::FDBInspect {

  public: // methods

    FDBDump(int argc, char **argv) :
        fdb5::FDBInspect(argc, argv),
        simple_(false) {

        options_.push_back(new eckit::option::SimpleOption<bool>("simple", "Dump one (simpler) record per line"));
    }

  private: // methods

    virtual void usage(const std::string &tool) const;
    virtual void init(const eckit::option::CmdArgs &args);
    virtual void process(const eckit::PathName &path, const eckit::option::CmdArgs &args);

    bool simple_;
};

void FDBDump::init(const eckit::option::CmdArgs &args) {
    args.get("simple", simple_);
}

void FDBDump::usage(const std::string &tool) const {
    fdb5::FDBInspect::usage(tool);
}

void FDBDump::process(const eckit::PathName &path, const eckit::option::CmdArgs &args) {

    eckit::Log::info() << "Dumping " << path << std::endl << std::endl;

    eckit::ScopedPtr<fdb5::DB> db(fdb5::DBFactory::buildReader(path));
    ASSERT(db->open());

    db->dump(eckit::Log::info(), simple_);

    // eckit::Log::info() << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBDump app(argc, argv);
    return app.start();
}


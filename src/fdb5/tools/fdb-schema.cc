/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/rules/Schema.h"
#include "fdb5/config/MasterConfig.h"
#include "eckit/option/CmdArgs.h"
#include "fdb5/tools/FDBAccess.h"


class FdbSchema : public fdb5::FDBAccess {
    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
public:
    FdbSchema(int argc, char **argv): fdb5::FDBAccess(argc, argv) {}
};

void FdbSchema::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " [shema] ..." << std::endl;
    fdb5::FDBAccess::usage(tool);
}

void FdbSchema:: execute(const eckit::option::CmdArgs &args) {

    fdb5::Schema schema;

    if (args.count() == 0) {
        schema.load(fdb5::MasterConfig::instance().schemaPath());
        schema.dump(std::cout);
    }

    for (size_t i = 0; i < args.count(); i++) {
        schema.load(args(i));
        schema.dump(std::cout);
    }
}


int main(int argc, char **argv) {
    FdbSchema app(argc, argv);
    return app.start();
}


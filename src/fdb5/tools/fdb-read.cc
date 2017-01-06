/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/io/FileHandle.h"
#include "eckit/memory/ScopedPtr.h"
#include "eckit/option/CmdArgs.h"

#include "fdb5/database/Retriever.h"
#include "fdb5/grib/GribDecoder.h"
#include "fdb5/tools/FDBAccess.h"
#include "fdb5/tools/RequestParser.h"

#include "marslib/MarsTask.h"


class FDBRead : public fdb5::FDBAccess {
    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int numberOfPositionalArguments() const { return 2; }
  public:
    FDBRead(int argc, char **argv): fdb5::FDBAccess(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<bool>("extract", "Extract request from a GRIB file"));

    }
};

void FDBRead::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " request.mars target.grib" << std::endl
                       << "       " << tool << " --extract source.grib target.grib" << std::endl;

    fdb5::FDBAccess::usage(tool);
}

void FDBRead::execute(const eckit::option::CmdArgs &args) {

    bool extract = false;
    args.get("extract", extract);

    MarsRequest r;

    if (extract) {
        fdb5::GribDecoder decoder;
        r = decoder.gribToRequest(args(0));
    } else {
        std::ifstream in(args(0).c_str());
        if (in.bad()) {
            throw eckit::ReadError(args(0));
        }
        fdb5::RequestParser parser(in);
        r = parser.parse();
    }

    std::cout << r << std::endl;

    MarsRequest e("environ");

    MarsTask task(r, e);

    fdb5::Retriever retriever;

    eckit::ScopedPtr<DataHandle> dh ( retriever.retrieve(task) );

    eckit::FileHandle out( args(1));

    dh->saveInto(out);
}


int main(int argc, char **argv) {
    FDBRead app(argc, argv);
    return app.start();
}


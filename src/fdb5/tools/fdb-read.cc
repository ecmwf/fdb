/*
 * (C) Copyright 1996- ECMWF.
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

#include "fdb5/api/FDB.h"
#include "fdb5/grib/GribDecoder.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/tools/FDBTool.h"
#include "fdb5/tools/RequestParser.h"

#include "marslib/MarsRequest.h"


class FDBRead : public fdb5::FDBTool {
    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int numberOfPositionalArguments() const { return 2; }
  public:
    FDBRead(int argc, char **argv): fdb5::FDBTool(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<bool>("extract", "Extract request from a GRIB file"));

        options_.push_back(
                    new eckit::option::SimpleOption<bool>("statistics",
                                                          "Report timing statistics"));
    }
};

void FDBRead::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " request.mars target.grib" << std::endl
                       << "       " << tool << " --extract source.grib target.grib" << std::endl;

    fdb5::FDBTool::usage(tool);
}

void FDBRead::execute(const eckit::option::CmdArgs &args) {

    bool extract = false;
    args.get("extract", extract);

    std::vector<MarsRequest> requests;

    // Build request(s) from input

    eckit::FileHandle out(args(1));

    if (extract) {

        fdb5::GribDecoder decoder;
        requests = decoder.gribToRequests(args(0));

    } else {
        std::ifstream in(args(0).c_str());
        if (in.bad()) {
            throw eckit::ReadError(args(0));
        }
        fdb5::RequestParser parser(in);
        requests.push_back(parser.parse());
    }

    // Evaluate the requests to obtain data handles

    fdb5::HandleGatherer handles(false);

    fdb5::FDB fdb(args);

    for (std::vector<MarsRequest>::const_iterator rit = requests.begin(); rit != requests.end(); ++rit) {

        eckit::Log::info() << (*rit) << std::endl;

        handles.add(fdb.retrieve(*rit));
    }

    // And get the data

    eckit::ScopedPtr<eckit::DataHandle> dh(handles.dataHandle());

    dh->saveInto(out);
}


int main(int argc, char **argv) {
    FDBRead app(argc, argv);
    return app.start();
}


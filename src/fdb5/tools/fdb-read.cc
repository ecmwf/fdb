/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <fstream>
#include <memory>

#include "eckit/io/FileHandle.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "metkit/mars/MarsExpansion.h"
#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsRequest.h"

#include "fdb5/api/FDB.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/message/MessageDecoder.h"
#include "fdb5/tools/FDBTool.h"


class FDBRead : public fdb5::FDBTool {
    virtual void execute(const eckit::option::CmdArgs& args);
    virtual void usage(const std::string& tool) const;
    virtual int numberOfPositionalArguments() const { return 2; }

public:

    FDBRead(int argc, char** argv) : fdb5::FDBTool(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<bool>("extract", "Extract request from a GRIB file"));
        options_.push_back(new eckit::option::SimpleOption<bool>("raw", "Uses the raw request, without expansion"));
        options_.push_back(new eckit::option::SimpleOption<bool>("statistics", "Report timing statistics"));
    }
};

void FDBRead::usage(const std::string& tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " request.mars target.grib" << std::endl
                       << "       " << tool << " --raw request.mars target.grib" << std::endl
                       << "       " << tool << " --extract source.grib target.grib" << std::endl;

    fdb5::FDBTool::usage(tool);
}

void FDBRead::execute(const eckit::option::CmdArgs& args) {

    bool extract = args.getBool("extract", false);
    bool raw     = args.getBool("raw", false);

    std::vector<metkit::mars::MarsRequest> requests;

    // Build request(s) from input

    eckit::FileHandle out(args(1));

    if (extract) {

        fdb5::MessageDecoder decoder;
        requests = decoder.messageToRequests(args(0));
    }
    else {
        std::ifstream in(args(0).c_str());
        if (in.bad()) {
            throw eckit::ReadError(args(0));
        }

        metkit::mars::MarsParser parser(in);
        auto parsedRequests = parser.parse();
        if (raw) {
            for (auto r : parsedRequests)
                requests.push_back(r);
        }
        else {
            metkit::mars::MarsExpansion expand(/* inherit */ false);
            requests = expand.expand(parsedRequests);
        }
    }

    // Evaluate the requests to obtain data handles
    fdb5::HandleGatherer handles(true);
    {
        fdb5::FDB fdb{config(args)};

        for (const auto& request : requests) {
            eckit::Log::info() << request << std::endl;

            handles.add(fdb.retrieve(request));
        }
    }

    // And get the data
    std::unique_ptr<eckit::DataHandle> dh(handles.dataHandle());

    dh->saveInto(out);
}


int main(int argc, char** argv) {
    FDBRead app(argc, argv);
    return app.start();
}

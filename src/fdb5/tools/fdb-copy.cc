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

#include "eckit/filesystem/PathName.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "metkit/mars/MarsExpension.h"
#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsRequest.h"

#include "fdb5/api/FDB.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/message/MessageArchiver.h"
#include "fdb5/tools/FDBTool.h"

using namespace eckit::option;

class FDBCopy : public fdb5::FDBTool {
    void execute(const CmdArgs& args) override;
    void usage(const std::string& tool) const override;

public:

    FDBCopy(int argc, char** argv) : fdb5::FDBTool(argc, argv) {
        options_.push_back(new SimpleOption<bool>("verbose", "Print verbose output"));
        options_.push_back(new SimpleOption<bool>("raw", "Process the MARS request without expansion"));
        options_.push_back(new SimpleOption<bool>("sort", "Sort fields according to location on input storage"));
        options_.push_back(new SimpleOption<eckit::PathName>("to", "Configuration of FDB to write to"));
        options_.push_back(new SimpleOption<eckit::PathName>("from", "Configuration of FDB to read from"));
    }
};

void FDBCopy::usage(const std::string& tool) const {
    eckit::Log::info() << std::endl << "Usage: " << tool << " --from <config> --to <config> <request1>" << std::endl;
    fdb5::FDBTool::usage(tool);
}

static std::vector<metkit::mars::MarsRequest> readRequest(const CmdArgs& args) {

    std::vector<metkit::mars::MarsRequest> requests;

    for (size_t i = 0; i < args.count(); ++i) {

        std::ifstream in(args(i).c_str());
        if (in.bad()) {
            std::ostringstream msg;
            msg << "Failed to read request file " << args(0);
            throw eckit::ReadError(msg.str());
        }

        metkit::mars::MarsParser parser(in);
        auto parsedRequests = parser.parse();

        if (args.getBool("raw", false)) {
            for (auto r : parsedRequests)
                requests.push_back(r);
        }
        else {
            const bool inherit = false;
            metkit::mars::MarsExpension expand(inherit);
            auto expanded = expand.expand(parsedRequests);
            requests.insert(requests.end(), expanded.begin(), expanded.end());
        }
    }
    return requests;
}

void FDBCopy::execute(const CmdArgs& args) {

    bool verbose = args.getBool("verbose", false);

    std::string from;
    args.get("from", from);
    if (from.empty()) {
        throw eckit::UserError("Missing --from parameter");
    }

    std::string to;
    args.get("to", to);
    if (to.empty()) {
        throw eckit::UserError("Missing --to parameter");
    }

    fdb5::Config readConfig  = fdb5::Config::make(eckit::PathName(from));
    fdb5::Config writeConfig = fdb5::Config::make(eckit::PathName(to));

    std::vector<metkit::mars::MarsRequest> requests = readRequest(args);

    // Evaluate the requests to obtain data handles
    const bool sort = args.getBool("sort", false);
    fdb5::HandleGatherer handles(sort);

    fdb5::FDB fdbRead(readConfig);

    for (const auto& request : requests) {
        eckit::Log::info() << request << std::endl;
        handles.add(fdbRead.retrieve(request));
    }

    std::unique_ptr<eckit::DataHandle> dh(handles.dataHandle());

    fdb5::MessageArchiver fdbWriter(fdb5::Key(), false, verbose, writeConfig);
    fdbWriter.archive(*dh);
}

int main(int argc, char** argv) {
    FDBCopy app(argc, argv);
    return app.start();
}

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

#include "metkit/mars/MarsExpansion.h"
#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsRequest.h"

#include "fdb5/api/FDB.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/message/MessageArchiver.h"
#include "fdb5/tools/FDBVisitTool.h"

using namespace eckit::option;
using namespace eckit;

namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBCopy : public fdb5::tools::FDBVisitTool {

    bool verbose_ = false;
    bool fromList_ = false;
    bool sort_ = false;

    std::string modifiers_;

    eckit::PathName sourceConfig_ = {};
    eckit::PathName targetConfig_ = {};

    void checkModifiers(const metkit::mars::MarsRequest&, const eckit::StringDict&);
    void execute(const CmdArgs& args) override;
    void usage(const std::string& tool) const override;
    void init(const CmdArgs& args) override;

public:

    FDBCopy(int argc, char** argv);
};

FDBCopy::FDBCopy(int argc, char** argv) : fdb5::tools::FDBVisitTool(argc, argv, "class,expver") {
    options_.push_back(new SimpleOption<bool>("verbose", "Print verbose output"));
    options_.push_back(new SimpleOption<bool>("sort", "Sort fields according to location on input storage"));
    options_.push_back(new SimpleOption<eckit::PathName>("target", "Configuration of FDB to write to"));
    options_.push_back(new SimpleOption<eckit::PathName>("source", "Configuration of FDB to read from"));
    options_.push_back(new eckit::option::SimpleOption<bool>(
        "from-list", "Interpret argument(s) as fdb-list partial requests, rather than request files"));
    options_.push_back(new eckit::option::SimpleOption<std::string>(
        "modifiers",
        "List of comma separated key-values of modifiers to each message "
        "in input data. The modifier keys must also be present in the "
        "supplied request. Example: --modifiers=expver=0042,date=20190603"));
}

void FDBCopy::usage(const std::string& tool) const {

    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " --source <config> --target <config> [options] <request1>" << std::endl
                       << std::endl;

    Log::info() << "Examples:" << std::endl
                << "=========" << std::endl
                << std::endl
                << tool << " --source=config_from.yaml --target=config_to.yaml requests.mars" << std::endl
                << tool
                << " --source=config_from.yaml --target=config_to.yaml --from-list "
                   "class=rd,expver=xywz,stream=oper,date=20190603,time=00"
                << std::endl
                << std::endl;

    // n.b. we do NOT want to use FDBVisitTool::usage()
    FDBTool::usage(tool);
}

void FDBCopy::init(const CmdArgs& args) {

    FDBVisitTool::init(args);

    args.get("modifiers", modifiers_);

    verbose_ = args.getBool("verbose", verbose_);
    fromList_ = args.getBool("from-list", fromList_);
    sort_ = args.getBool("sort", sort_);

    std::string from = args.getString("source");
    if (from.empty()) {
        throw eckit::UserError("Missing --source parameter", Here());
    }
    sourceConfig_ = from;

    std::string to = args.getString("target");
    if (to.empty()) {
        throw eckit::UserError("Missing --target parameter", Here());
    }
    targetConfig_ = to;
}

static std::vector<metkit::mars::MarsRequest> readRequestsFromFile(const CmdArgs& args) {

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
            for (auto r : parsedRequests) {
                requests.push_back(r);
            }
        }
        else {
            const bool inherit = false;
            metkit::mars::MarsExpansion expand(inherit);
            auto expanded = expand.expand(parsedRequests);
            requests.insert(requests.end(), expanded.begin(), expanded.end());
        }
    }
    return requests;
}

void FDBCopy::checkModifiers(const metkit::mars::MarsRequest& request, const eckit::StringDict& modifiers) {
    for (const auto& pair : modifiers) {
        if (!request.has(pair.first)) {
            std::ostringstream msg;
            msg << "Provided modifiers for key '" << pair.first << "' not present in data to be copied";
            throw eckit::UserError(msg.str());
        }
    }
}

void FDBCopy::execute(const CmdArgs& args) {

    fdb5::Config readConfig = fdb5::Config::make(sourceConfig_);
    fdb5::Config writeConfig = fdb5::Config::make(targetConfig_);

    fdb5::HandleGatherer handles(sort_);
    fdb5::FDB fdbRead(readConfig);

    // parse modifiers if any
    fdb5::Key modifiers = fdb5::Key::parse(modifiers_);

    if (fromList_) {
        for (const FDBToolRequest& request : requests("list")) {
            checkModifiers(request.request(), modifiers);
            bool deduplicate = true;
            auto listObject = fdbRead.list(request, deduplicate);
            handles.add(fdbRead.read(listObject, sort_));
        }
    }
    else {
        std::vector<metkit::mars::MarsRequest> requests = readRequestsFromFile(args);
        for (const auto& request : requests) {
            checkModifiers(request, modifiers);
            eckit::Log::info() << request << std::endl;
            handles.add(fdbRead.retrieve(request));
        }
    }

    std::unique_ptr<eckit::DataHandle> dh(handles.dataHandle());

    fdb5::MessageArchiver fdbWriter(fdb5::Key(), false, verbose_, writeConfig);
    fdbWriter.setModifiers(modifiers);
    fdbWriter.archive(*dh);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char** argv) {
    fdb5::tools::FDBCopy app(argc, argv);
    return app.start();
}

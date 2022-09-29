/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

#include "fdb5/tools/FDBVisitTool.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/LibFdb5.h"

#define MAX_THREADS 256

using namespace eckit::option;
using namespace eckit;

namespace fdb5 {
namespace tools {

//----------------------------------------------------------------------------------------------------------------------

class FDBMove : public FDBVisitTool {
public: // methods

    FDBMove(int argc, char **argv);
    ~FDBMove() override;

private: // methods

    void execute(const CmdArgs& args) override;
    void init(const CmdArgs &args) override;

private: // members

    eckit::URI destination_;
    bool keep_;
    int removeDelay_;
    int threads_;
};

FDBMove::FDBMove(int argc, char **argv) :
    FDBVisitTool(argc, argv, "class,expver,stream,date,time"),
    destination_(""),
    keep_(false),
    removeDelay_(0),
    threads_(1) {

    options_.push_back(new SimpleOption<std::string>("dest", "Destination root"));
    options_.push_back(new SimpleOption<bool>("keep", "Keep source DB"));
    options_.push_back(new SimpleOption<long>("delay", "Delay in seconds before deleting source (default: 0)"));
    options_.push_back(new SimpleOption<long>("threads", "Number of concurrent threads for data move (default: 1)"));
}

FDBMove::~FDBMove() {}


void FDBMove::init(const CmdArgs& args) {

    FDBVisitTool::init(args);

    keep_ = args.getBool("keep", false);
    removeDelay_ = args.getInt("delay", 0);
    threads_ = args.getInt("threads", 1);

    std::string dest = args.getString("dest");
    if (dest.empty()) {
        std::stringstream ss;
        ss << "No destination root specified.";
        throw UserError(ss.str(), Here());
    } else {
        destination_ = eckit::URI(dest);
    }

    if (threads_ < 1 || threads_ > MAX_THREADS) {
        std::stringstream ss;
        ss << "Unsupported number of threads. please specify a value between 1 and " << MAX_THREADS;
        throw UserError(ss.str(), Here());
    } else {
        destination_ = eckit::URI(dest);
    }

}


void FDBMove::execute(const CmdArgs& args) {

    FDB fdb(config(args));

    size_t count = 0;
    for (const FDBToolRequest& request : requests("read")) {
        if (count) {
            std::stringstream ss;
            ss << "Multiple requests are not supported" << std::endl;
            throw eckit::UserError(ss.str());
        }

        if (request.all()) {
            std::stringstream ss;
            ss << "Move ALL not supported. Please specify a single database." << std::endl;
            throw eckit::UserError(ss.str(), Here());
        }

        // check that the request is only referring a single DB - no ranges of values
        const metkit::mars::MarsRequest& req = request.request();
        std::vector<std::string> params = req.params();
        for (const std::string& param: params) {
            const std::vector<std::string>& values = req.values(param);

            if (values.size() != 1) {
                std::stringstream ss;
                ss << "Move requires a single value for each parameter in the request." << std::endl << "Parameter " << param << "=" << values << " not supported." << std::endl;
                throw eckit::UserError(ss.str(), Here());
            }
        }

        // check that exaclty one DB matches
        StatsIterator it = fdb.stats(request);
        StatsElement se;
        if (!it.next(se)) {
            std::stringstream ss;
            ss << "Request " << req << " does not matches with an existing database. Please specify a single database." << std::endl;
            throw eckit::UserError(ss.str(), Here());
        }
        if (it.next(se)) {
            std::stringstream ss;
            ss << "Request " << req << " matches with more than one existing database. Please specify a single database." << std::endl;
            throw eckit::UserError(ss.str(), Here());
        }

        MoveIterator list = fdb.move(request, destination_, !keep_, removeDelay_, threads_);
        MoveElement elem;
        while (list.next(elem)) {
            Log::info() << elem << std::endl;
        }
        count++;
    }

    if (count == 0 && fail()) {
        std::stringstream ss;
        ss << "No FDB entries found" << std::endl;
        throw FDBToolException(ss.str());
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5

int main(int argc, char **argv) {
    fdb5::tools::FDBMove app(argc, argv);
    return app.start();
}

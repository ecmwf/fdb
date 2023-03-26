/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "eckit/mpi/Comm.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/serialisation/ResizableMemoryStream.h"

#include "fdb5/tools/FDBVisitTool.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/toc/TocCommon.h"

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
    bool mpi_;
    int rank_;
};

FDBMove::FDBMove(int argc, char **argv) :
    FDBVisitTool(argc, argv, "class,expver,stream,date,time"),
    destination_(""),
    keep_(false),
    removeDelay_(0),
    threads_(1),
    mpi_(false),
    rank_(-1) {

    options_.push_back(new SimpleOption<std::string>("dest", "Destination root"));
    options_.push_back(new SimpleOption<bool>("keep", "Keep source DB"));
    options_.push_back(new SimpleOption<long>("delay", "Delay in seconds before deleting source (default: 0)"));
    options_.push_back(new SimpleOption<bool>("mpi", "distributed data move (MPI based)"));
    options_.push_back(new SimpleOption<long>("threads", "Number of concurrent threads for data move (default: 1)"));
}

FDBMove::~FDBMove() {}


void FDBMove::init(const CmdArgs& args) {

    FDBVisitTool::init(args);

    keep_ = args.getBool("keep", false);
    mpi_ = args.getBool("mpi", false);
    removeDelay_ = args.getInt("delay", 0);

    eckit::mpi::Comm& comm = eckit::mpi::comm();

    if (mpi_) {
        threads_ = 1;
        rank_ = int(comm.rank());
    } else {
        threads_ = args.getInt("threads", 1);
    }

    if (!mpi_ || rank_ == 0) {
        std::string dest = args.getString("dest");
        if (dest.empty()) {
            std::stringstream ss;
            ss << "No destination root specified.";
            throw UserError(ss.str(), Here());
        } else {
            destination_ = eckit::URI(dest);
        }

        if (mpi_ || (0 < threads_ && threads_ <= MAX_THREADS)) {
            destination_ = eckit::URI(dest);
        } else {
            std::stringstream ss;
            ss << "Unsupported number of threads. please specify a value between 1 and " << MAX_THREADS;
            throw UserError(ss.str(), Here());
        }
    } else { // this process is just a data mover - subscribing to receive tasks

        while (true) {
            int ready = 1;

            comm.template send<int>(&ready, 1, 0, 0);

            eckit::mpi::Status st = comm.probe(0, 0);
            size_t size = comm.getCount<char>(st);

            if (size>0) {
                eckit::Buffer b(size);
                b.zero();

                comm.receive(static_cast<char*>(b.data()), b.size(), 0, 0);
                eckit::ResizableMemoryStream s(b);

                FileCopy fileCopy(s);
                Log::debug() << "fdb-move (mover " << rank_ << ") received task " << fileCopy << std::endl;

                fileCopy.execute();
            } else {
                Log::debug() << "fdb-move (mover " << rank_ << ") done" << std::endl;
                break;
            }
        }
    }
}


void FDBMove::execute(const CmdArgs& args) {

    if (!mpi_ || rank_ == 0) {
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

            MoveIterator list = fdb.move(request, destination_, !keep_, removeDelay_, mpi_, threads_);
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
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace tools
} // namespace fdb5

int main(int argc, char **argv) {
    fdb5::tools::FDBMove app(argc, argv);
    return app.start();
}

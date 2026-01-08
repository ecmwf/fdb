/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <unistd.h>

#include "eckit/distributed/Consumer.h"
#include "eckit/distributed/Message.h"
#include "eckit/distributed/Producer.h"
#include "eckit/distributed/Transport.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/thread/ThreadPool.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBVisitTool.h"

#define MAX_THREADS 256

using namespace eckit;
using namespace eckit::option;

namespace fdb5::tools {

//----------------------------------------------------------------------------------------------------------------------


struct fdb_moveiterator_t {
public:

    fdb_moveiterator_t(fdb5::MoveIterator&& iter) : iter_(std::move(iter)) {}

    bool next(fdb5::MoveElement& elem) { return iter_.next(elem); }

private:

    fdb5::MoveIterator iter_;
};


class MoveProducer : public eckit::distributed::Producer {
public:  // methods

    MoveProducer(eckit::distributed::Transport& transport, const Config& config,
                 const std::vector<fdb5::FDBToolRequest>& requests, const eckit::option::CmdArgs& args) :
        eckit::distributed::Producer(transport), fdb_(config), keep_(false), removeDelay_(0) {

        keep_        = args.getBool("keep", false);
        removeDelay_ = args.getInt("delay", 0);

        eckit::URI destination;
        std::string dest = args.getString("dest");
        if (dest.empty()) {
            std::ostringstream ss;
            ss << "No destination root specified.";
            throw UserError(ss.str(), Here());
        }
        else {
            destination = eckit::URI(dest);
        }

        fdb5::FDBToolRequest request = metkit::mars::MarsRequest();
        size_t count                 = 0;
        for (const FDBToolRequest& toolReq : requests) {
            if (count) {
                std::ostringstream ss;
                ss << "Multiple requests are not supported" << std::endl;
                throw eckit::UserError(ss.str());
            }

            if (toolReq.all()) {
                std::ostringstream ss;
                ss << "Move ALL not supported. Please specify a single database." << std::endl;
                throw eckit::UserError(ss.str(), Here());
            }

            // check that the request is only referring a single DB - no ranges of values
            const metkit::mars::MarsRequest& marsReq = toolReq.request();
            std::vector<std::string> params          = marsReq.params();
            for (const std::string& param : params) {
                const std::vector<std::string>& values = marsReq.values(param);

                if (values.size() != 1) {
                    std::ostringstream ss;
                    ss << "Move requires a single value for each parameter in the request." << std::endl
                       << "Parameter " << param << "=" << values << " not supported." << std::endl;
                    throw eckit::UserError(ss.str(), Here());
                }
            }

            // check that exaclty one DB matches
            StatsIterator it = fdb_.stats(toolReq);
            StatsElement se;
            if (!it.next(se)) {
                std::ostringstream ss;
                ss << "Request " << toolReq
                   << " does not matches with an existing database. Please specify a single database." << std::endl;
                throw eckit::UserError(ss.str(), Here());
            }
            if (it.next(se)) {
                std::ostringstream ss;
                ss << "Request " << toolReq
                   << " matches with more than one existing database. Please specify a single database." << std::endl;
                throw eckit::UserError(ss.str(), Here());
            }

            request = toolReq;
            count++;
        }

        if (count == 0) {
            std::ostringstream ss;
            ss << "No FDB entries found" << std::endl;
            throw FDBToolException(ss.str());
        }

        LOG_DEBUG_LIB(LibFdb5) << "Request:     " << request << std::endl;
        LOG_DEBUG_LIB(LibFdb5) << "Destination: " << destination << std::endl;

        moveIterator_ = std::make_unique<fdb_moveiterator_t>(fdb_.move(request, destination));
    }
    ~MoveProducer() {}

private:  // methods

    virtual bool produce(eckit::distributed::Message& message) {
        ASSERT(moveIterator_);

        fdb5::MoveElement elem;
        if (moveIterator_->next(elem)) {
            LOG_DEBUG_LIB(LibFdb5) << "MoveProducer " << elem << std::endl;
            if (!elem.sync()) {
                list_.push_back(elem);
                elem.encode(message);
                return true;
            }
            last_ = elem;
        }
        return false;
    }
    virtual void finalise() {
        transport_.synchronise();
        last_.execute();

        if (!keep_) {
            last_.cleanup();

            sleep(removeDelay_);

            for (auto& el : list_) {
                el.cleanup();
            }

            fdb5::MoveElement elem;
            while (moveIterator_->next(elem)) {
                elem.cleanup();
            }
        }
    }

    void messageFromWorker(eckit::distributed::Message& message, int worker) const {}

private:  // attributes

    fdb5::FDB fdb_;
    fdb5::MoveElement last_;
    std::vector<fdb5::MoveElement> list_;
    std::unique_ptr<fdb_moveiterator_t> moveIterator_;

    bool keep_;
    int removeDelay_;
};


class MoveWorker : public eckit::distributed::Consumer {

public:  // methods

    MoveWorker(eckit::distributed::Transport& transport, const eckit::option::CmdArgs& args) :
        eckit::distributed::Consumer(transport), count_(0) {}
    ~MoveWorker() {}

protected:  // members

    void consume(eckit::distributed::Message& message) override {
        fdb5::FileCopy fileCopy(message);
        LOG_DEBUG_LIB(LibFdb5) << "MoveWorker " << fileCopy << std::endl;

        count_++;
        fileCopy.execute();
    }
    void finalise() override { transport_.synchronise(); };

    // virtual void getNextMessage(eckit::Message& message) const;
    // virtual void failure(eckit::Message &message);
    void shutdown(eckit::distributed::Message& message) override {
        LOG_DEBUG_LIB(LibFdb5) << "Shuting down MoveWorker..." << std::endl;
        message << count_;
    }

    void getNextMessage(eckit::distributed::Message& message) const override { getNextWorkMessage(message); }

private:  // attributes

    size_t count_;
};

//----------------------------------------------------------------------------------------------------------------------

class MoveLoner : public eckit::distributed::Actor {
public:  // methods

    MoveLoner(eckit::distributed::Transport& transport, eckit::distributed::Producer* producer,
              eckit::distributed::Consumer* consumer, size_t numThreads = 1) :
        eckit::distributed::Actor(transport), producer_(producer), consumer_(consumer), numThreads_(numThreads) {}

private:  // methods

    virtual void run() {
        eckit::distributed::Message message;

        eckit::distributed::Producer& producer = *producer_;
        eckit::distributed::Consumer& consumer = *consumer_;

        eckit::ThreadPool pool("move", numThreads_);

        while (producer.produce(message)) {
            message.rewind();
            try {
                pool.push(new FileCopy(message));
            }
            catch (eckit::Exception& e) {
                eckit::Log::error() << e.what() << std::endl;
                consumer.failure(message);
            }
            message.rewind();
        }
        pool.wait();

        message.rewind();
        consumer.shutdown(message);
        message.rewind();
        producer.messageFromWorker(message, 0);
    }

    virtual void finalise() {
        eckit::distributed::Producer& producer = *producer_;
        eckit::distributed::Consumer& consumer = *consumer_;

        consumer.finalise();
        producer.finalise();
    }

private:  // members

    std::unique_ptr<eckit::distributed::Producer> producer_;
    std::unique_ptr<eckit::distributed::Consumer> consumer_;
    size_t numThreads_;
};

//----------------------------------------------------------------------------------------------------------------------


class FDBMove : public FDBVisitTool {
public:  // methods

    FDBMove(int argc, char** argv);
    ~FDBMove() override;

private:  // methods

    void execute(const CmdArgs& args) override;
    void init(const CmdArgs& args) override;

private:  // members

    std::unique_ptr<eckit::distributed::Transport> transport_;
    int threads_;
};

FDBMove::FDBMove(int argc, char** argv) : FDBVisitTool(argc, argv, "class,expver,stream,date,time") {

    options_.push_back(new SimpleOption<std::string>("dest", "Destination root"));
    options_.push_back(new SimpleOption<bool>("keep", "Keep source DB"));
    options_.push_back(new SimpleOption<long>("delay", "Delay in seconds before deleting source (default: 0)"));
    options_.push_back(new SimpleOption<std::string>("transport", "distributed data move (MPI based)"));
    options_.push_back(new SimpleOption<long>("threads", "Number of concurrent threads for data move (default: 1)"));
}

FDBMove::~FDBMove() {}

void FDBMove::init(const CmdArgs& args) {

    FDBVisitTool::init(args);
}


void FDBMove::execute(const CmdArgs& args) {

    std::unique_ptr<eckit::distributed::Transport> transport(eckit::distributed::TransportFactory::build(args));

    try {
        std::unique_ptr<eckit::distributed::Actor> actor;

        if (transport->single()) {
            threads_ = args.getInt("threads", 1);
            if (threads_ <= 0 || MAX_THREADS < threads_) {
                std::ostringstream ss;
                ss << "Unsupported number of threads. please specify a value between 1 and " << MAX_THREADS;
                throw UserError(ss.str(), Here());
            }

            actor.reset(new MoveLoner(*transport, new MoveProducer(*transport, config(args), requests("read"), args),
                                      new MoveWorker(*transport, args),
                                      threads_));  // do it all actor :)
        }
        else {
            if (transport->producer()) {
                actor.reset(new MoveProducer(*transport, config(args), requests("read"), args));  // work dispatcher
            }
            else {
                actor.reset(new MoveWorker(*transport, args));  // worker
            }
        }

        actor->run();

        actor->finalise();

        // eckit::Log::info() << eckit::TimeStamp()
        //                    << " "
        //                    << title
        //                    << ": "
        //                    << eckit::system::ResourceUsage()
        //                    << std::endl;
    }
    catch (std::exception& e) {
        eckit::Log::info()
            //    << eckit::TimeStamp()
            //    << " "
            //    << title
            << " EXCEPTION: " << e.what() << std::endl;
        transport->abort();
        throw;
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::tools

int main(int argc, char** argv) {
    fdb5::tools::FDBMove app(argc, argv);
    return app.start();
}

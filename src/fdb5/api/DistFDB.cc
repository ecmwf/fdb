/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <vector>
#include <thread>
#include <future>

#include "eckit/config/LocalConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"
#include "eckit/utils/Tokenizer.h"

#include "fdb5/api/DistFDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/LibFdb5.h"

using eckit::Log;


namespace fdb5 {

static FDBBuilder<DistFDB> distFdbBuilder("dist");

//----------------------------------------------------------------------------------------------------------------------


struct DistributionError : public eckit::Exception {
    DistributionError(const std::string& r, const eckit::CodeLocation& loc) :
        Exception(std::string(" DistributionError: " + r, loc)) {}
};

//----------------------------------------------------------------------------------------------------------------------

DistFDB::DistFDB(const eckit::Configuration& config, const std::string& name) :
    FDBBase(config, name) {

    ASSERT(config.getString("type", "") == "dist");

    // Configure the available lanes.

    if (!config.has("lanes")) throw eckit::UserError("No lanes configured for pool", Here());

    std::vector<eckit::LocalConfiguration> laneConfigs;
    laneConfigs = config.getSubConfigurations("lanes");

    for(const eckit::LocalConfiguration& laneCfg : laneConfigs) {
        lanes_.push_back(FDB(laneCfg));
        if (!hash_.addNode(lanes_.back().id())) {
            std::stringstream ss;
            ss << "Failed to add node to hash : " << lanes_.back().id() << " -- may have non-unique ID";
            throw eckit::SeriousBug(ss.str(), Here());
        }
    }
}

DistFDB::~DistFDB() {}


void DistFDB::archive(const Key& key, const void* data, size_t length) {

    std::vector<size_t> laneIndices;

    Log::debug<LibFdb5>() << "Number of lanes: " << lanes_.size() << std::endl;
    Log::debug<LibFdb5>() << "Lane indices: ";
    for (const auto& i : laneIndices) Log::debug<LibFdb5>() << i << ", ";
    Log::debug<LibFdb5>() << std::endl;

    hash_.hashOrder(key.keyDict(), laneIndices);

    Log::debug<LibFdb5>() << "Number of lanes: " << lanes_.size() << std::endl;
    Log::debug<LibFdb5>() << "Lane indices: ";
    for (const auto& i : laneIndices) Log::debug<LibFdb5>() << i << ", ";
    Log::debug<LibFdb5>() << std::endl;

    // Given an order supplied by the Rendezvous hash, try the FDB in order until
    // one works. n.b. Errors are unacceptable once the FDB is dirty.
    Log::debug<LibFdb5>() << "Attempting dist FDB archive" << std::endl;

    for (size_t idx : laneIndices) {

        FDB& lane = lanes_[idx];

        if(not lane.writable()) continue;

        if (lane.disabled()) {
            eckit::Log::warning() << "FDB lane " << lane << " is disabled" << std::endl;
            continue;
        }

        try {

            lane.archive(key, data, length);
            return;

        } catch (eckit::Exception& e) {

            // TODO: This will be messy and verbose. Reduce output if it has already failed.

            std::stringstream ss;
            ss << "Archive failure on lane: " << lane << " (" << idx << ")";
            eckit::Log::error() << ss.str() << std::endl;
            eckit::Log::error() << "with exception: " << e << std::endl;

            // If we have written, but not flushed, data to a give lane, and an archive operation
            // fails, then this is a bit of an issue. Otherwise, just skip the lane.

            if (lane.dirty()) {
                ss << " -- Exception: " << e;
                throw DistributionError(ss.str(), Here());
            }

            // Mark the lane as no longer writable
            lane.disable();
        }
    }

    Log::error() << "No writable lanes!!!!" << std::endl;

    throw DistributionError("No writable lanes available for archive", Here());
}

eckit::DataHandle* DistFDB::retrieve(const metkit::MarsRequest &request) {

    // TODO: Deduplication. Currently no masking.
    // TODO: Error handling on read.

//    HandleGatherer result(true); // Sorted
    HandleGatherer result(false);

    for (FDB& lane : lanes_) {
        if (lane.visitable()) {
            result.add(lane.retrieve(request));
        }
    }

    return result.dataHandle();
}

/*
 * Exemplar for templated query functionality:
 *
 * ListIterator DistFDB::list(const FDBToolRequest& request) {
 *
 *     std::queue<ListIterator> lists;
 *
 *     for (FDB& lane : lanes_) {
 *         if (lane.visitable()) {
 *             lists.push(lane.list(request));
 *         }
 *     }
 *
 *     return ListIterator(new ListAggregateIterator(std::move(lists)));
 * }
 */


template <typename QueryFN>
auto DistFDB::queryInternal(const FDBToolRequest& request, const QueryFN& fn) -> decltype(fn(*(FDB*)(nullptr), request)) {

    using QueryIterator = decltype(fn(*(FDB*)(nullptr), request));

    std::queue<QueryIterator> iterQueue;

    for (FDB& lane : lanes_) {
        if (lane.visitable()) {
            iterQueue.push(fn(lane, request));
        }
    }

    return QueryIterator(new APIAggregateIterator<typename QueryIterator::value_type>(std::move(iterQueue)));
}


ListIterator DistFDB::list(const FDBToolRequest& request) {
    Log::debug<LibFdb5>() << "DistFDB::list() : " << request << std::endl;
    return queryInternal(request,
                         [](FDB& fdb, const FDBToolRequest& request) {
                            return fdb.list(request);
                         });
}

DumpIterator DistFDB::dump(const FDBToolRequest& request, bool simple) {
    Log::debug<LibFdb5>() << "DistFDB::dump() : " << request << std::endl;
    return queryInternal(request,
                         [simple](FDB& fdb, const FDBToolRequest& request) {
                            return fdb.dump(request, simple);
                         });
}

WhereIterator DistFDB::where(const FDBToolRequest& request) {
    Log::debug<LibFdb5>() << "DistFDB::where() : " << request << std::endl;
    return queryInternal(request,
                         [](FDB& fdb, const FDBToolRequest& request) {
                            return fdb.where(request);
    });
}

WipeIterator DistFDB::wipe(const FDBToolRequest& request, bool doit, bool verbose) {
    Log::debug<LibFdb5>() << "DistFDB::wipe() : " << request << std::endl;
    return queryInternal(request,
                         [doit, verbose](FDB& fdb, const FDBToolRequest& request) {
                            return fdb.wipe(request, doit, verbose);
    });
}

PurgeIterator DistFDB::purge(const FDBToolRequest& request, bool doit, bool verbose) {
    Log::debug<LibFdb5>() << "DistFDB::purge() : " << request << std::endl;
    return queryInternal(request,
                         [doit, verbose](FDB& fdb, const FDBToolRequest& request) {
                            return fdb.purge(request, doit, verbose);
    });
}

StatsIterator DistFDB::stats(const FDBToolRequest &request) {
    Log::debug<LibFdb5>() << "DistFDB::stats() : " << request << std::endl;
    return queryInternal(request,
                         [](FDB& fdb, const FDBToolRequest& request) {
                            return fdb.stats(request);
    });
}


void DistFDB::flush() {

    std::vector<std::thread> threads;
    std::vector<std::promise<int>> promises(lanes_.size());
    std::vector<std::future<int>> futures;

    for (size_t i = 0; i < lanes_.size(); i++) {

        FDB& lane(lanes_[i]);
        std::promise<int>& prm(promises[i]);
        futures.emplace_back(prm.get_future());

        threads.emplace_back(std::thread([&lane, &prm]{
            try {
                lane.flush();
                prm.set_value(0);
            } catch (...) {
                prm.set_exception(std::current_exception());
            }
        }));
    }

    for (auto& fut : futures) {
        fut.get();
    }

    for (std::thread& thread : threads) {
        ASSERT(thread.joinable());
        thread.join();
    }
}

FDBStats DistFDB::stats() const {
    FDBStats s;
    for (const auto& lane : lanes_) {
        s += lane.internalStats();
    }
    return s;
}


void DistFDB::print(std::ostream &s) const {
    s << "DistFDB(home=" << config_.expandPath("~fdb") << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

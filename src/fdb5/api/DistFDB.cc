/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include <future>
#include <thread>
#include <vector>

#include "eckit/config/LocalConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"
#include "eckit/message/Message.h"
#include "eckit/utils/Tokenizer.h"

#include "metkit/hypercube/HyperCube.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/DistFDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/database/Notifier.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/io/HandleGatherer.h"

using eckit::Log;


namespace fdb5 {

static FDBBuilder<DistFDB> distFdbBuilder("dist");

//----------------------------------------------------------------------------------------------------------------------


struct DistributionError : public eckit::Exception {
    DistributionError(const std::string& r, const eckit::CodeLocation& loc) :
        Exception(std::string(" DistributionError: " + r, loc)) {}
};

//----------------------------------------------------------------------------------------------------------------------

DistFDB::DistFDB(const Config& config, const std::string& name) : FDBBase(config, name) {

    ASSERT(config.getString("type", "") == "dist");

    // Configure the available lanes.

    if (!config.has("lanes"))
        throw eckit::UserError("No lanes configured for pool", Here());

    for (const auto& laneCfg : config.getSubConfigs("lanes")) {
        lanes_.emplace_back(FDB(laneCfg), true);
        if (!hash_.addNode(std::get<0>(lanes_.back()).id())) {
            std::ostringstream ss;
            ss << "Failed to add node to hash : " << std::get<0>(lanes_.back()).id() << " -- may have non-unique ID";
            throw eckit::SeriousBug(ss.str(), Here());
        }
    }
}

DistFDB::~DistFDB() {}

void DistFDB::archive(const Key& key, const void* data, size_t length) {

    std::vector<size_t> laneIndices;

    // LOG_DEBUG_LIB(LibFdb5) << "Number of lanes: " << lanes_.size() << std::endl;
    // LOG_DEBUG_LIB(LibFdb5) << "Lane indices: ";
    // for (const auto& i : laneIndices) LOG_DEBUG_LIB(LibFdb5) << i << ", ";
    // LOG_DEBUG_LIB(LibFdb5) << std::endl;

    hash_.hashOrder(key.keyDict(), laneIndices);

    // LOG_DEBUG_LIB(LibFdb5) << "Number of lanes: " << lanes_.size() << std::endl;
    // LOG_DEBUG_LIB(LibFdb5) << "Lane indices: ";
    // for (const auto& i : laneIndices) LOG_DEBUG_LIB(LibFdb5) << i << ", ";
    // LOG_DEBUG_LIB(LibFdb5) << std::endl;

    // Given an order supplied by the Rendezvous hash, try the FDB in order until
    // one works. n.b. Errors are unacceptable once the FDB is dirty.
    LOG_DEBUG_LIB(LibFdb5) << "Attempting dist FDB archive" << std::endl;

    decltype(laneIndices)::const_iterator it  = laneIndices.begin();
    decltype(laneIndices)::const_iterator end = laneIndices.end();
    for (; it != end; ++it) {
        size_t idx = *it;

        auto& [lane, enabled] = lanes_[idx];

        if (!lane.enabled(ControlIdentifier::Archive)) {
            continue;
        }
        if (!enabled) {
            eckit::Log::warning() << "FDB lane " << lane << " is disabled" << std::endl;
            continue;
        }

        try {

            lane.archive(key, data, length);
            return;
        }
        catch (eckit::Exception& e) {

            // TODO: This will be messy and verbose. Reduce output if it has already failed.

            std::ostringstream ss;
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
            enabled = false;
        }
    }

    Log::error() << "No writable lanes!!!!" << std::endl;

    throw DistributionError("No writable lanes available for archive", Here());
}

template <typename QueryFN>
auto DistFDB::queryInternal(const FDBToolRequest& request, const QueryFN& fn)
    -> decltype(fn(*(FDB*)(nullptr), request)) {

    using QueryIterator = decltype(fn(*(FDB*)(nullptr), request));
    using ValueType     = typename QueryIterator::value_type;

    std::vector<std::future<QueryIterator>> futures;
    std::queue<APIIterator<ValueType>> iterQueue;

    for (auto& [lane, _] : lanes_) {
        // Note(kkratz): Lambdas cannot capture structured bindings prior to C++20, hence we must introduce a alias
        // here.
        FDB& cap_lane = lane;
        if (lane.enabled(ControlIdentifier::Retrieve)) {
            futures.emplace_back(
                std::async(std::launch::async, [&lane = cap_lane, &fn, &request] { return fn(lane, request); }));
        }
    }

    for (std::future<QueryIterator>& f : futures) {
        iterQueue.push(f.get());
    }

    return QueryIterator(new APIAggregateIterator<ValueType>(std::move(iterQueue)));
}


ListIterator DistFDB::list(const FDBToolRequest& request, int level) {
    LOG_DEBUG_LIB(LibFdb5) << "DistFDB::list() : " << request << std::endl;
    return queryInternal(request, [level](FDB& fdb, const FDBToolRequest& request) {
        bool deduplicate = false;  // never deduplicate on inner calls
        return fdb.list(request, deduplicate, level);
    });
}

ListIterator DistFDB::inspect(const metkit::mars::MarsRequest& request) {
    LOG_DEBUG_LIB(LibFdb5) << "DistFDB::inspect() : " << request << std::endl;
    return queryInternal(request,
                         [](FDB& fdb, const FDBToolRequest& request) { return fdb.inspect(request.request()); });
}

DumpIterator DistFDB::dump(const FDBToolRequest& request, bool simple) {
    LOG_DEBUG_LIB(LibFdb5) << "DistFDB::dump() : " << request << std::endl;
    return queryInternal(request,
                         [simple](FDB& fdb, const FDBToolRequest& request) { return fdb.dump(request, simple); });
}

StatusIterator DistFDB::status(const FDBToolRequest& request) {
    LOG_DEBUG_LIB(LibFdb5) << "DistFDB::status() : " << request << std::endl;
    return queryInternal(request, [](FDB& fdb, const FDBToolRequest& request) { return fdb.status(request); });
}

// DIST FDB WILL BE REMOVED IN A FUTURE UPDATE
WipeStateIterator DistFDB::wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) {
    LOG_DEBUG_LIB(LibFdb5) << "DistFDB::wipe() : " << request << std::endl;
    NOTIMP;
}

PurgeIterator DistFDB::purge(const FDBToolRequest& request, bool doit, bool porcelain) {
    LOG_DEBUG_LIB(LibFdb5) << "DistFDB::purge() : " << request << std::endl;
    return queryInternal(request, [doit, porcelain](FDB& fdb, const FDBToolRequest& request) {
        return fdb.purge(request, doit, porcelain);
    });
}

StatsIterator DistFDB::stats(const FDBToolRequest& request) {
    LOG_DEBUG_LIB(LibFdb5) << "DistFDB::stats() : " << request << std::endl;
    return queryInternal(request, [](FDB& fdb, const FDBToolRequest& request) { return fdb.stats(request); });
}

ControlIterator DistFDB::control(const FDBToolRequest& request, ControlAction action, ControlIdentifiers identifiers) {
    LOG_DEBUG_LIB(LibFdb5) << "DistFDB::control() : " << request << std::endl;
    return queryInternal(request, [action, identifiers](FDB& fdb, const FDBToolRequest& request) {
        return fdb.control(request, action, identifiers);
    });
}


MoveIterator DistFDB::move(const FDBToolRequest& request, const eckit::URI& dest) {
    LOG_DEBUG_LIB(LibFdb5) << "DistFDB::move() : " << request << std::endl;
    return queryInternal(request, [dest](FDB& fdb, const FDBToolRequest& request) { return fdb.move(request, dest); });
}

void DistFDB::flush() {

    std::vector<std::future<void>> futures;

    for (auto& [lane, _] : lanes_) {
        // Note(kkratz): Lambdas cannot capture structured bindings prior to C++20, hence we must introduce a alias
        // here.
        FDB& cap_lane = lane;
        futures.emplace_back(std::async(std::launch::async, [&lane = cap_lane] { lane.flush(); }));
    }
}

void DistFDB::print(std::ostream& s) const {
    s << "DistFDB(home=" << config_.expandPath("~fdb") << ")";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

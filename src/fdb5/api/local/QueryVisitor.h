/*
 * (C) Copyright 2018- ECMWF.
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

/// @author Simon Smart
/// @date   November 2018

#pragma once

#include "eckit/container/Queue.h"
#include "eckit/container/QueueOfQueues.h"
#include "eckit/exception/Exceptions.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/rules/Rule.h"

#include <memory>
#include <mutex>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace fdb5::api::local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

template <typename T, typename Q = eckit::Queue<T>>
class QueryVisitor : public EntryVisitor {

public:  // methods

    using ValueType = T;
    using QueueType = Q;

    QueryVisitor(QueueType& queue, const metkit::mars::MarsRequest& request) : queue_(queue), request_(request) {}

    using EntryVisitor::visitIndex;

    /// If running in parallel, and the ordering of output matters, then we will have a per-thread
    /// per-index output queue. We need to take that, and close it appropriately.
    ///
    /// We should always release the ordering imposed by the OrderedParallelFor once everything that
    /// requires strict index ordering is done. Do this in this class, such that we only have
    /// to implement this logic once, no matter how many visitors can be parallelised.
    IndexScopePtr visitIndex(const Index& index, OrderedParallelFor::Order& order) final {

        const Rule& rule = indexRule(index);

        eckit::Queue<ValueType>& queue = obtainQueue();  // the one thing that must happen in order
        order.release();

        if constexpr (ordered()) {
            // A sub-queue that is never closed stalls the consumer for good. A returned scope
            // adopts it; if the visitor skips this index or throws, close it here instead, leaving
            // an empty place in the sequence.
            try {
                auto scope = visitIndex(index, rule, queue);
                if (!scope) {
                    queue.close();
                }
                return scope;
            }
            catch (...) {
                queue.close();
                throw;
            }
        }
        else {
            return visitIndex(index, rule, queue);
        }
    }

    virtual IndexScopePtr visitIndex(const Index& index, const Rule& rule, eckit::Queue<ValueType>& queue) {
        return EntryVisitor::visitIndex(index, rule);
    }

protected:  // methods

    /// output sequencing uses a QueueOfQueues. So this can be used to switch if the output is sequenced
    static constexpr bool ordered() { return std::is_same_v<QueueType, eckit::QueueOfQueues<ValueType>>; }
    eckit::Queue<ValueType>& obtainQueue() {
        if constexpr (ordered()) {
            return queue_.push();
        }
        else {
            return queue_;
        }
    }

    const metkit::mars::MarsRequest& canonicalise(const Rule& rule) const {
        bool success;
        std::lock_guard<std::mutex> lock(canonicalisedMutex_);
        auto it = canonicalised_.find(&rule.registry());
        if (it == canonicalised_.end()) {
            std::tie(it, success) = canonicalised_.emplace(&rule.registry(), rule.registry().canonicalise(request_));
            ASSERT(success);
        }
        return it->second;
    }


protected:  // members

    QueueType& queue_;
    metkit::mars::MarsRequest request_;

private:  // members

    /// Cache of canonicalised requests
    mutable std::unordered_map<const TypesRegistry*, metkit::mars::MarsRequest> canonicalised_;
    mutable std::mutex canonicalisedMutex_;  ///< Protects canonicalised_ map
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local

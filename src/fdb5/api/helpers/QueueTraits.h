/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   September 2026

#pragma once

#include "eckit/container/Queue.h"
#include "eckit/container/QueueOfQueues.h"

#include "fdb5/config/Config.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// Describes how to build different types of queue from configuration
///
/// @note make() returns a prvalue, so the queue is constructed directly into its final storage.
///   Therefore acceptable under C++17, even though Queues are non-movable

template <typename Q>
struct QueueTraits;

template <typename T>
struct QueueTraits<eckit::Queue<T>> {
    static eckit::Queue<T> make(const Config& config) { return eckit::Queue<T>(config.apiQueueSize()); }
};

template <typename T>
struct QueueTraits<eckit::QueueOfQueues<T>> {
    static eckit::QueueOfQueues<T> make(const Config& config) {
        return eckit::QueueOfQueues<T>(config.apiMaxQueues(), config.apiQueueSize());
    }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

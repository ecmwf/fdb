/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include <cstddef>
#include <functional>
#include <mutex>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// Runs indexed work items across several threads, each entered in turn so that it can take an
/// ordered share of a resource before the rest of it runs concurrently.
///
/// work() is entered in strictly ascending item order and never concurrently with another item's
/// entry; once an item releases its Order, work() proceeds concurrently with everything else. The
/// calling thread is one of the workers, so nthreads == 1 spawns no threads and runs the batch in
/// order on the caller.

class OrderedParallelFor {

public:  // types

    /// Held by the item that is currently next in line.
    ///
    /// Release it as soon as the item has taken whatever it must take in item order - a
    /// QueueOfQueues sub-queue, say - so that output emitted concurrently afterwards still lands
    /// in item order. Taking the item number and entering work() are a single atomic step, which
    /// is what makes the ordering hold.
    ///
    /// Held until release() or, failing that, until work() returns: forgetting to release costs
    /// concurrency, never correctness. Taking it may block - that delays the items behind, never
    /// those already in flight.
    class Order {

    public:  // methods

        /// An Order that holds nothing, for callers running outside a parallel batch. release()
        /// is a no-op.
        Order() = default;

        Order(const Order&) = delete;
        Order& operator=(const Order&) = delete;

        /// Idempotent.
        void release() {
            if (lock_ && lock_->owns_lock()) {
                lock_->unlock();
            }
        }

    private:  // methods

        friend class OrderedParallelFor;
        explicit Order(std::unique_lock<std::mutex>& lock) : lock_(&lock) {}

    private:  // members

        std::unique_lock<std::mutex>* lock_{nullptr};
    };

    /// Called once per item, on the thread that entered it in turn.
    using Work = std::function<void(size_t item, Order& order)>;

public:  // methods

    explicit OrderedParallelFor(size_t nthreads);

    OrderedParallelFor(const OrderedParallelFor&) = delete;
    OrderedParallelFor& operator=(const OrderedParallelFor&) = delete;

    void run(size_t n, const Work& work);

private:  // members

    size_t nthreads_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

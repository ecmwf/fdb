/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/OrderedParallelFor.h"

#include "eckit/exception/Exceptions.h"

#include <algorithm>
#include <atomic>
#include <exception>
#include <mutex>
#include <thread>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {

struct State {

    /// Guards next and the ordered head of work() together. Taking the item number and entering
    /// work() must be one atomic step: were the counter incremented outside this lock, two threads
    /// could take items 5 and 6 and then take their ordered resources in either order.
    ///
    /// Held across the head of work(), which may block. That is safe as long as whatever it waits
    /// on is released by work() after its Order, which no longer holds this lock.
    std::mutex claimMutex;
    size_t next{0};

    /// Read outside the lock to break out early, but only ever set under it where work() threw
    /// before releasing, so such a failure is seen by the next thread to reach the claim.
    std::atomic<bool> failed{false};
    std::vector<std::exception_ptr> errors;

    void recordFailure(size_t item, std::exception_ptr e) {
        if (!errors[item]) {
            errors[item] = std::move(e);
        }
        failed.store(true, std::memory_order_release);
    }
};

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

OrderedParallelFor::OrderedParallelFor(size_t nthreads) : nthreads_(nthreads) {
    ASSERT(nthreads_ >= 1);
}

void OrderedParallelFor::run(size_t n, const Work& work) {

    if (n == 0) {
        return;
    }

    State state;
    state.errors.resize(n);

    const size_t nthreads = std::min(nthreads_, n);

    auto worker = [&] {
        for (;;) {

            std::unique_lock<std::mutex> lock(state.claimMutex);

            if (state.failed.load(std::memory_order_acquire)) {
                break;
            }

            const size_t item = state.next;
            if (item >= n) {
                break;
            }
            ++state.next;

            // n.b. we pass the lock INTO the worker, which can release it. This enables the worker
            //      to sequence access to a controlled resource, and then run in parallel after.
            Order order(lock);
            try {
                work(item, order);
            }
            catch (...) {
                // Should work() have thrown before releasing, we still hold the lock, so this is
                // seen by the next thread to reach the claim and nothing further is started.
                state.recordFailure(item, std::current_exception());
            }
        }
    };

    {
        std::vector<std::thread> pool;
        pool.reserve(nthreads - 1);

        struct Joiner {
            std::vector<std::thread>& pool;
            ~Joiner() {
                for (auto& thread : pool) {
                    if (thread.joinable()) {
                        thread.join();
                    }
                }
            }
        } joiner{pool};

        for (size_t t = 1; t < nthreads; ++t) {
            pool.emplace_back(worker);
        }

        worker();  // the calling thread is a worker too

        for (auto& thread : pool) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    for (const auto& error : state.errors) {
        if (error) {
            std::rethrow_exception(error);
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

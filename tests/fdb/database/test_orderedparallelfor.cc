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

#include "eckit/container/QueueOfQueues.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/testing/Test.h"

#include <atomic>
#include <chrono>
#include <numeric>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

using fdb5::OrderedParallelFor;
using Order = fdb5::OrderedParallelFor::Order;

namespace {

const std::vector<size_t> THREAD_COUNTS{1, 2, 4, 8};

//----------------------------------------------------------------------------------------------------------------------

CASE("Every item runs exactly once, for any thread count") {

    for (size_t nthreads : THREAD_COUNTS) {
        constexpr size_t n = 200;
        std::vector<std::atomic<int>> runs(n);
        for (auto& r : runs) {
            r = 0;
        }

        OrderedParallelFor(nthreads).run(n, [&](size_t i, Order& order) {
            order.release();
            ++runs[i];
        });

        for (size_t i = 0; i < n; ++i) {
            EXPECT_EQUAL(runs[i].load(), 1);
        }
    }
}

CASE("work() is entered in ascending item order, and never concurrently before release()") {

    // This is the contract the whole design rests on: whatever an item takes before releasing - a
    // QueueOfQueues sub-queue, in practice - is taken in item order, which is what puts
    // concurrently produced output back in order.
    for (size_t nthreads : THREAD_COUNTS) {
        constexpr size_t n = 200;

        std::vector<size_t> entryOrder;
        std::atomic<int> inOrderedSection{0};
        std::atomic<int> maxInOrderedSection{0};

        OrderedParallelFor(nthreads).run(n, [&](size_t i, Order& order) {
            const int now = ++inOrderedSection;
            int previousMax = maxInOrderedSection.load();
            while (now > previousMax && !maxInOrderedSection.compare_exchange_weak(previousMax, now)) {}
            entryOrder.push_back(i);  // unsynchronised on purpose: the ordered head must be exclusive
            --inOrderedSection;

            order.release();

            // Staggered so later items routinely finish before earlier ones.
            std::this_thread::sleep_for(std::chrono::microseconds((n - i) % 17));
        });

        EXPECT_EQUAL(maxInOrderedSection.load(), 1);
        EXPECT_EQUAL(entryOrder.size(), n);

        std::vector<size_t> expected(n);
        std::iota(expected.begin(), expected.end(), 0);
        EXPECT(entryOrder == expected);
    }
}

CASE("After release(), work() runs concurrently") {

    // The counterpart to the case above: ordering the head must not order the tail. A design that
    // held the ordering for the whole of work() would pass the ordering test and fail this one.
    constexpr size_t n = 16;
    std::atomic<int> inTail{0};
    std::atomic<int> maxInTail{0};

    OrderedParallelFor(8).run(n, [&](size_t, Order& order) {
        order.release();

        const int now = ++inTail;
        int previousMax = maxInTail.load();
        while (now > previousMax && !maxInTail.compare_exchange_weak(previousMax, now)) {}
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        --inTail;
    });

    EXPECT(maxInTail.load() > 1);
}

CASE("Work that never releases is serialised, but still correct") {

    // Forgetting to release costs concurrency, never correctness - the Order is released when
    // work() returns however it ends. QueryVisitor marks its ordered overload final precisely so
    // that no visitor can land here by accident.
    constexpr size_t n = 32;
    std::vector<size_t> ran;

    OrderedParallelFor(8).run(n, [&](size_t i, Order& /*never released*/) {
        ran.push_back(i);  // unsynchronised: safe only because this is fully serialised
    });

    std::vector<size_t> expected(n);
    std::iota(expected.begin(), expected.end(), 0);
    EXPECT(ran == expected);
}

CASE("nthreads == 1 runs entirely on the calling thread") {

    constexpr size_t n = 50;
    const auto caller = std::this_thread::get_id();
    std::set<std::thread::id> threads;

    OrderedParallelFor(1).run(n, [&](size_t, Order& order) {
        order.release();
        threads.insert(std::this_thread::get_id());
    });

    EXPECT_EQUAL(threads.size(), 1);
    EXPECT(*threads.begin() == caller);
}

CASE("A default-constructed Order holds nothing") {

    // What the non-parallel callers - TocCatalogueWriter's reindex loop - hand to a visitor.
    Order order;
    EXPECT_NO_THROW(order.release());
    EXPECT_NO_THROW(order.release());  // idempotent
}

CASE("The lowest-indexed error is reported, whichever ran first, and nothing deadlocks") {

    for (size_t nthreads : THREAD_COUNTS) {
        constexpr size_t n = 100;
        std::vector<size_t> entered;

        // Two failures; the lower-indexed must be reported, whichever ran first.
        auto call = [&] {
            OrderedParallelFor(nthreads).run(n, [&](size_t i, Order& order) {
                entered.push_back(i);
                order.release();

                if (i == 40) {
                    throw std::runtime_error("item 40");
                }
                if (i == 20) {
                    throw std::runtime_error("item 20");
                }
            });
        };

        try {
            call();
            EXPECT(false);  // must have thrown
        }
        catch (const std::runtime_error& e) {
            EXPECT_EQUAL(std::string(e.what()), std::string("item 20"));
        }

        // What was entered is an ascending prefix, so nothing behind the failure was stranded.
        for (size_t k = 0; k < entered.size(); ++k) {
            EXPECT_EQUAL(entered[k], k);
        }

        // n.b. no assertion that the batch was abandoned early. A throw *after* release races the
        // other threads, which with a trivial tail can drain the rest before the failure
        // registers. Fail-fast is pinned deterministically by the case below, where the throw
        // happens while the ordering is still held.
    }
}

CASE("A throw before release() is recorded under the ordering, so nothing further is started") {

    for (size_t nthreads : THREAD_COUNTS) {
        constexpr size_t n = 40;
        std::vector<size_t> entered;

        try {
            OrderedParallelFor(nthreads).run(n, [&](size_t i, Order& order) {
                entered.push_back(i);
                if (i == 10) {
                    throw std::runtime_error("item 10");  // still holding the ordering
                }
                order.release();
            });
            EXPECT(false);
        }
        catch (const std::runtime_error& e) {
            EXPECT_EQUAL(std::string(e.what()), std::string("item 10"));
        }

        // The failure is recorded while the ordering is still held, so the next thread to reach
        // the claim sees it: exactly the eleven items up to and including the failure are started.
        EXPECT_EQUAL(entered.size(), size_t(11));
        for (size_t k = 0; k < entered.size(); ++k) {
            EXPECT_EQUAL(entered[k], k);
        }
    }
}

CASE("A blocking acquisition before release() does not deadlock against work() in flight") {

    // Mirrors what Catalogue::visitEntries does: the ordered head takes a bounded resource that
    // only the concurrent tail releases. This only makes progress because the tail has released
    // the ordering.
    constexpr size_t n = 64;
    constexpr int capacity = 3;
    std::atomic<int> outstanding{0};
    std::atomic<int> maxOutstanding{0};

    OrderedParallelFor(8).run(n, [&](size_t, Order& order) {
        while (outstanding.load() >= capacity) {
            std::this_thread::yield();
        }
        const int now = ++outstanding;
        int previousMax = maxOutstanding.load();
        while (now > previousMax && !maxOutstanding.compare_exchange_weak(previousMax, now)) {}

        order.release();

        std::this_thread::sleep_for(std::chrono::microseconds(50));
        --outstanding;
    });

    EXPECT(maxOutstanding.load() <= capacity);
    EXPECT_EQUAL(outstanding.load(), 0);
}

//----------------------------------------------------------------------------------------------------------------------
// The combination with QueueOfQueues, as Catalogue::visitEntries() assembles it. These are what
// actually protect fdb-list: ordered output, and - the failure mode that hangs rather than errors -
// a consumer that still terminates when an index throws part-way through.

namespace {

/// Mirrors Catalogue::visitEntries(): take a sub-queue while still ordered, release, then fill and
/// close it. `fail` throws after emitting some of its output, as a failing index does.
void runVisitation(size_t nthreads, size_t n, size_t perItem, eckit::QueueOfQueues<int>& queues,
                   size_t fail = static_cast<size_t>(-1)) {

    OrderedParallelFor(nthreads).run(n, [&](size_t i, Order& order) {
        eckit::Queue<int>& queue = queues.push();
        order.release();

        // Closes the sub-queue however this ends - the anti-deadlock guarantee.
        struct Closer {
            eckit::Queue<int>& queue;
            ~Closer() { queue.close(); }
        } closer{queue};

        for (size_t k = 0; k < perItem; ++k) {
            queue.push(static_cast<int>(i * 1000 + k));
        }
        if (i == fail) {
            throw std::runtime_error("item failed");
        }
    });
}

}  // namespace

CASE("Concurrent visitation emits in item order through a QueueOfQueues") {

    for (size_t nthreads : THREAD_COUNTS) {
        constexpr size_t n = 40;
        constexpr size_t perItem = 25;

        eckit::QueueOfQueues<int> queues(2 * nthreads, 4);  // tight, so producers must interleave

        std::vector<int> out;
        std::thread consumer([&] {
            int value = 0;
            while (queues.pop(value) != -1) {
                out.push_back(value);
            }
        });

        runVisitation(nthreads, n, perItem, queues);
        queues.close();
        consumer.join();

        std::vector<int> expected;
        for (size_t i = 0; i < n; ++i) {
            for (size_t k = 0; k < perItem; ++k) {
                expected.push_back(static_cast<int>(i * 1000 + k));
            }
        }
        EXPECT(out == expected);
    }
}

CASE("An item that throws part-way still lets the consumer terminate") {

    // A sub-queue left unclosed stalls the consumer for good, so this would hang rather than fail.
    for (size_t nthreads : THREAD_COUNTS) {
        constexpr size_t n = 40;

        eckit::QueueOfQueues<int> queues(2 * nthreads, 4);

        std::atomic<bool> drained{false};
        std::vector<int> out;
        std::thread consumer([&] {
            int value = 0;
            while (queues.pop(value) != -1) {
                out.push_back(value);
            }
            drained = true;
        });

        EXPECT_THROWS_AS(runVisitation(nthreads, n, 5, queues, /* fail */ 7), std::runtime_error);

        queues.close();
        consumer.join();
        EXPECT(drained.load());

        // Everything the failing item emitted before throwing is committed, as it is sequentially.
        EXPECT(out.size() >= 8 * 5);
        for (size_t k = 0; k < 8 * 5; ++k) {
            EXPECT_EQUAL(out[k], static_cast<int>((k / 5) * 1000 + (k % 5)));
        }
    }
}

CASE("Interrupting the output unwinds the visitation rather than hanging") {

    constexpr size_t n = 400;
    eckit::QueueOfQueues<int> queues(4, 2);  // small, so producers block and must be woken

    std::thread consumer([&] {
        int value = 0;
        for (int i = 0; i < 10; ++i) {  // consume a little, then abandon - as a dropped iterator does
            if (queues.pop(value) == -1) {
                return;
            }
        }
        queues.interrupt(std::make_exception_ptr(std::runtime_error("consumer went away")));
    });

    EXPECT_THROWS_AS(runVisitation(4, n, 50, queues), std::exception);
    consumer.join();
}

CASE("Degenerate inputs") {

    bool ran = false;
    OrderedParallelFor(4).run(0, [&](size_t, Order&) { ran = true; });
    EXPECT(!ran);

    // More threads than items.
    std::vector<size_t> entered;
    OrderedParallelFor(16).run(3, [&](size_t i, Order& order) {
        entered.push_back(i);
        order.release();
    });
    EXPECT_EQUAL(entered.size(), 3);
    EXPECT(entered == (std::vector<size_t>{0, 1, 2}));

    // nthreads must be at least 1.
    EXPECT_THROWS_AS(OrderedParallelFor(0), eckit::AssertionFailed);
}

//----------------------------------------------------------------------------------------------------------------------

}  // anonymous namespace

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}

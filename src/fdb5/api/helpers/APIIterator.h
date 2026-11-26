// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date   November 2018

#pragma once

#include "eckit/container/Queue.h"

#include "fdb5/api/helpers/QueueTraits.h"
#include "fdb5/tracing/TraceContext.h"
#include "fdb5/tracing/Tracer.h"

#include <exception>
#include <functional>
#include <memory>
#include <queue>

/*
 * Given a standard, copyable, element, provide a mechanism for iterating over
 * functions that return it!
 */

namespace fdb5 {

class FDBBase;

//----------------------------------------------------------------------------------------------------------------------

template <typename ValueType>
class APIIteratorBase {

public:  // methods

    APIIteratorBase() {}
    virtual ~APIIteratorBase() {}

    virtual bool next(ValueType& elem) = 0;
};

//----------------------------------------------------------------------------------------------------------------------

template <typename ValueType>
class APIIterator {

public:  // types

    using value_type = ValueType;

public:  // methods

    APIIterator(APIIteratorBase<ValueType>* impl) : impl_(impl) {}

    APIIterator(APIIterator&&) noexcept = default;

    /// n.b. as on destruction, the implementation of the iteration replaced is over (e.g. an asynchronous worker
    /// cancelled and joined) before its span ends
    APIIterator& operator=(APIIterator&& rhs) noexcept {
        if (this != &rhs) {
            impl_ = std::move(rhs.impl_);
            span_ = std::move(rhs.span_);
        }
        return *this;
    }

    ~APIIterator() = default;

    /// Get the next element. Return false if at end
    bool next(ValueType& elem) {
        try {
            if (impl_ && impl_->next(elem)) {
                return true;
            }
        }
        catch (const std::exception& e) {
            span_.setStatus(tracing::StatusCode::Error, e.what());
            span_.end();
            throw;
        }
        span_.end();
        return false;
    }

    /// Attach the tracing span of the API call that created this iterator. The span ends when the iteration is
    /// exhausted, fails, or the iterator is destroyed.
    void attachSpan(tracing::Span&& span) {
        span_ = std::move(span);
        // the iterator may be destroyed while the caller is unwinding for unrelated reasons: that is not a failure
        span_.detectUnwinding(false);
    }

private:  // members

    /// n.b. declared before impl_, so destroyed after it: the span of an abandoned iteration ends once its
    /// implementation is over (e.g. an asynchronous worker cancelled and joined), and its trace with it
    tracing::Span span_;

    std::unique_ptr<APIIteratorBase<ValueType>> impl_;
};

//----------------------------------------------------------------------------------------------------------------------

// Combine together a sequence of APIIterator objects into one

template <typename ValueType>
class APIAggregateIterator : public APIIteratorBase<ValueType> {

public:  // methods

    APIAggregateIterator(std::queue<APIIterator<ValueType>>&& iterators) : iterators_(std::move(iterators)) {}

    ~APIAggregateIterator() override {}

    bool next(ValueType& elem) override {

        while (!iterators_.empty()) {
            if (iterators_.front().next(elem)) {
                return true;
            }

            iterators_.pop();
        }

        return false;
    }

private:  // members

    std::queue<APIIterator<ValueType>> iterators_;
};


//----------------------------------------------------------------------------------------------------------------------

/// AsyncIterationCancellation is used to indicate a worker needs to be cancelled.
/// AsyncIterationCancellation is send through eckit::Queue::interrupt to the writing thread and cancels the task via
/// unwinding.
class AsyncIterationCancellation : public eckit::Exception {};

// For some uses, we have a generator function (i.e. through a visitor
// pattern). We want to invert the control such that the next element
// is generated on request.
//
// --> Use a (mutex protected) queue.
// --> Producer/consumer relationship

// the APIAsyncIterator holds a shared_ptr<FDBBase>, so if the FDB object goes out-of-scope,
// the actual implementation is kept alive until the iterator has been fully consumed

template <typename ValueType, typename QueueType = eckit::Queue<ValueType>>
class APIAsyncIterator : public APIIteratorBase<ValueType> {

public:  // methods

    /// The queue is sized from the configuration. How that is done depends on the QueueType - a
    /// QueueOfQueues is bounded in two dimensions where a Queue is bounded in one - so it is
    /// delegated to QueueTraits rather than spelled out at each call site.

    APIAsyncIterator(std::shared_ptr<FDBBase> fdb, std::function<void(QueueType&)> workerFn, const Config& config) :
        fdb_(fdb), queue_(QueueTraits<QueueType>::make(config)) {

        // The worker runs on behalf of the caller: propagate the caller's trace context to the worker thread
        // Add a call to set_done() on the eckit::Queue.
        auto fullWorker = [workerFn, this, traceContext = tracing::current()] {
            tracing::ScopedContext traceScope(traceContext);
            try {
                workerFn(queue_);
                queue_.close();
            }
            catch (const AsyncIterationCancellation&) {
                // WorkerFn has been cancelled, nothing to do.
            }
            catch (...) {
                // Really avoid calling std::terminate on worker thread.
                queue_.interrupt(std::current_exception());
            }
        };

        workerThread_ = std::thread(fullWorker);
    }

    ~APIAsyncIterator() override {
        if (!queue_.closed()) {
            // Cancel worker operation by causing a throw on next emplace/push/resize operation.
            queue_.interrupt(std::make_exception_ptr(AsyncIterationCancellation()));
        }
        ASSERT(workerThread_.joinable());
        workerThread_.join();
    }

    bool next(ValueType& elem) override { return !(queue_.pop(elem) == -1); }

private:  // members

    std::shared_ptr<FDBBase> fdb_;

    QueueType queue_;

    std::thread workerThread_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

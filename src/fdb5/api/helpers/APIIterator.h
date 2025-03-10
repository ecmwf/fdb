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

/// @author Simon Smart
/// @date   November 2018

#ifndef fdb5_helpers_APIIteratorBase_H
#define fdb5_helpers_APIIteratorBase_H

#include "eckit/container/Queue.h"

#include <exception>
#include <functional>
#include <memory>
#include <queue>

/*
 * Given a standard, copyable, element, provide a mechanism for iterating over
 * functions that return it!
 */

namespace fdb5 {

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

    /// Get the next element. Return false if at end
    bool next(ValueType& elem) {
        if (!impl_)
            return false;
        return impl_->next(elem);
    }

private:  // members

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

template <typename ValueType>
class APIAsyncIterator : public APIIteratorBase<ValueType> {

public:  // methods

    APIAsyncIterator(std::function<void(eckit::Queue<ValueType>&)> workerFn, size_t queueSize = 100) :
        queue_(queueSize) {

        // Add a call to set_done() on the eckit::Queue.
        auto fullWorker = [workerFn, this] {
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

    eckit::Queue<ValueType> queue_;

    std::thread workerThread_;
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

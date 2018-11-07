/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/helpers/FDBAsyncListObject.h"

#include "eckit/config/Resource.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FDBAsyncListObject::FDBAsyncListObject(std::function<void (eckit::Queue<FDBListElement>&)> workerFn) :
    queue_(eckit::Resource<size_t>("fdb5AsyncListObjectQueueLen", 100)) {

    auto fullWorker = [workerFn, this] {
        workerFn(queue_);
        queue_.set_done();
    };

    workerThread_ = std::thread(fullWorker);
}

FDBAsyncListObject::~FDBAsyncListObject() {}

bool FDBAsyncListObject::next(FDBListElement& elem) {

    long nqueue = queue_.pop(elem);

    if (nqueue == -1) {
        workerThread_.join();
        return false;
    } else {
        return true;
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5


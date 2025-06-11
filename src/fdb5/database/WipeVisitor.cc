/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/WipeVisitor.h"
#include "fdb5/database/Store.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

WipeVisitor::WipeVisitor(const metkit::mars::MarsRequest& request, Store& store, eckit::Queue<WipeElement>& queue,
                         bool doit, bool porcelain, bool unsafeWipeAll) :
    EntryVisitor(), request_(request), store_(store), queue_(queue), doit_(doit), porcelain_(porcelain),
    unsafeWipeAll_(unsafeWipeAll) {}


WipeVisitor::~WipeVisitor() {}

const StoreWipeElements& WipeVisitor::storeWipeElements() const {
    // lazy loading storeWipeElements
    if (!storeWipeElements_) {
        storeWipeElements_ = std::cref(store_.wipeElements());
    }
    return storeWipeElements_.value().get();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

/*
 * (C) Copyright 2018- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/local/ControlVisitor.h"

#include "fdb5/database/Catalogue.h"

namespace fdb5::api::local {

//----------------------------------------------------------------------------------------------------------------------

ControlVisitor::ControlVisitor(eckit::Queue<ControlElement>& queue,
                               const metkit::mars::MarsRequest& request,
                               ControlAction action,
                               ControlIdentifiers identifiers) :
    QueryVisitor<ControlElement>(queue, request),
    action_(action),
    identifiers_(identifiers) {}


bool ControlVisitor::visitDatabase(const Catalogue& catalogue, const Store& store) {

    EntryVisitor::visitDatabase(catalogue, store);

    // Only lock/unlock things that match exactly.

    if (catalogue.key().match(request_)) {
        catalogue.control(action_, identifiers_);
        queue_.emplace(catalogue);
    }

    return true;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

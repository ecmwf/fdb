// SPDX-FileCopyrightText: 2018- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/api/local/ControlVisitor.h"

#include "fdb5/database/Catalogue.h"

namespace fdb5::api::local {

//----------------------------------------------------------------------------------------------------------------------

ControlVisitor::ControlVisitor(eckit::Queue<ControlElement>& queue, const metkit::mars::MarsRequest& request,
                               ControlAction action, ControlIdentifiers identifiers) :
    QueryVisitor<ControlElement>(queue, request), action_(action), identifiers_(identifiers) {}


bool ControlVisitor::visitDatabase(const Catalogue& catalogue) {

    EntryVisitor::visitDatabase(catalogue);

    // Only lock/unlock things that match exactly.

    if (catalogue.key().match(request_)) {
        catalogue.control(action_, identifiers_);
        queue_.emplace(catalogue);
    }

    return true;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local

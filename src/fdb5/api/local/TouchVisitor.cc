/*
 * (C) Copyright 2018- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/local/TouchVisitor.h"

#include <sstream>

#include "eckit/exception/Exceptions.h"

#include "fdb5/database/Catalogue.h"

namespace fdb5::api::local {

//----------------------------------------------------------------------------------------------------------------------

TouchVisitor::TouchVisitor(eckit::Queue<ControlElement>& queue, const metkit::mars::MarsRequest& request,
                           AccessIdentifiers identifiers, AccessTime when) :
    QueryVisitor(queue, request), identifiers_(identifiers), when_(when) {}

bool TouchVisitor::visitDatabase(const Catalogue& catalogue) {

    EntryVisitor::visitDatabase(catalogue);

    // Only touch databases that match _exactly_

    if (catalogue.key().match(request_)) {

        // Note that the automatic timestamp updating is best-effort, and failures are silenced. But
        // as fdb-touch is explicit, failures here should be noisy. So we promote the return code
        // into a proper exception.

        if (!catalogue.touch(identifiers_, when_)) {
            std::ostringstream ss;
            ss << "Updating timestamp failed for: " << catalogue.key();
            throw eckit::Exception(ss.str(), Here());
        }

        queue_.emplace(catalogue);
    }

    return true;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local

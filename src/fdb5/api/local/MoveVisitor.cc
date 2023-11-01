
/*
 * (C) Copyright 2018- ECMWF.
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

#include "fdb5/api/local/MoveVisitor.h"

#include "fdb5/api/local/QueueStringLogTarget.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/LibFdb5.h"

#include "eckit/os/Stat.h"

#include <sys/stat.h>
#include <dirent.h>

using namespace eckit;


namespace fdb5 {
namespace api {
namespace local {

//----------------------------------------------------------------------------------------------------------------------

MoveVisitor::MoveVisitor(eckit::Queue<MoveElement>& queue,
                         const metkit::mars::MarsRequest& request,
                         const eckit::URI& dest) :
    QueryVisitor<MoveElement>(queue, request),
    dest_(dest) {}

bool MoveVisitor::visitDatabase(const Catalogue& catalogue, const Store& store) {
    if (catalogue.key().match(request_)) {
        catalogue.control(
            ControlAction::Disable,
            ControlIdentifier::Archive | ControlIdentifier::Wipe | ControlIdentifier::UniqueRoot);

        // assert the source is locked for archival...
        ASSERT(!catalogue.enabled(ControlIdentifier::Archive));
        ASSERT(!catalogue.enabled(ControlIdentifier::Wipe));
        ASSERT(!catalogue.enabled(ControlIdentifier::UniqueRoot));

        EntryVisitor::visitDatabase(catalogue, store);

        ASSERT(!internalVisitor_);
        internalVisitor_.reset(catalogue.moveVisitor(store, request_, dest_, queue_));
        internalVisitor_->visitDatabase(catalogue, store);

        return true;
    }

    return false;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace local
} // namespace api
} // namespace fdb5

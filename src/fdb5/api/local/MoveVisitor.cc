
// SPDX-FileCopyrightText: 2018- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "fdb5/api/local/MoveVisitor.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/local/QueueStringLogTarget.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"

#include "eckit/os/Stat.h"

#include <dirent.h>
#include <sys/stat.h>

using namespace eckit;


namespace fdb5::api::local {

//----------------------------------------------------------------------------------------------------------------------

MoveVisitor::MoveVisitor(eckit::Queue<MoveElement>& queue, const metkit::mars::MarsRequest& request,
                         const eckit::URI& dest) :
    QueryVisitor<MoveElement>(queue, request), dest_(dest) {}

bool MoveVisitor::visitDatabase(const Catalogue& catalogue) {
    if (catalogue.key().match(request_)) {
        catalogue.control(ControlAction::Disable,
                          ControlIdentifier::Archive | ControlIdentifier::Wipe | ControlIdentifier::UniqueRoot);

        // assert the source is locked for archival...
        ASSERT(!catalogue.enabled(ControlIdentifier::Archive));
        ASSERT(!catalogue.enabled(ControlIdentifier::Wipe));
        ASSERT(!catalogue.enabled(ControlIdentifier::UniqueRoot));

        EntryVisitor::visitDatabase(catalogue);

        ASSERT(!internalVisitor_);
        internalVisitor_.reset(catalogue.moveVisitor(store(), request_, dest_, queue_));
        internalVisitor_->visitDatabase(catalogue);

        return true;
    }

    return false;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local

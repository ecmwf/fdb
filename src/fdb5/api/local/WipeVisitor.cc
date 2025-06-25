
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

#include "fdb5/api/local/WipeVisitor.h"

#include <dirent.h>
#include <sys/stat.h>

#include "eckit/os/Stat.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/local/QueueStringLogTarget.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"

using namespace eckit;


namespace fdb5 {
namespace api {
namespace local {

//----------------------------------------------------------------------------------------------------------------------

WipeVisitor::WipeVisitor(eckit::Queue<WipeElement>& queue, const metkit::mars::MarsRequest& request, bool doit,
                         bool porcelain, bool unsafeWipeAll) :
    QueryVisitor<WipeElement>(queue, request),
    out_(new QueueStringLogTarget(queue)),
    doit_(doit),
    porcelain_(porcelain),
    unsafeWipeAll_(unsafeWipeAll) {}


bool WipeVisitor::visitDatabase(const Catalogue& catalogue, const Store& store) {

    // If the DB is locked for wiping, then it "doesn't exist"
    if (!catalogue.enabled(ControlIdentifier::Wipe)) {
        return false;
    }

    EntryVisitor::visitDatabase(catalogue, store);

    ASSERT(!internalVisitor_);
    internalVisitor_.reset(catalogue.wipeVisitor(store, request_, out_, doit_, porcelain_, unsafeWipeAll_));
    internalVisitor_->visitDatabase(catalogue, store);

    return true;  // Explore contained indexes
}

bool WipeVisitor::visitIndex(const Index& index) {
    ASSERT(internalVisitor_);
    internalVisitor_->visitIndex(index);

    return false;  // Do not explore contained entries
}

void WipeVisitor::catalogueComplete(const Catalogue& catalogue) {
    ASSERT(internalVisitor_);
    internalVisitor_->catalogueComplete(catalogue);

    // Cleanup

    internalVisitor_.reset();
}

bool WipeVisitor::visitIndexes() {
    ASSERT(internalVisitor_);
    return internalVisitor_->visitIndexes();
}
//----------------------------------------------------------------------------------------------------------------------

}  // namespace local
}  // namespace api
}  // namespace fdb5

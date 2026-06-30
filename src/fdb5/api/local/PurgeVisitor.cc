
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

#include "fdb5/api/local/PurgeVisitor.h"

#include "eckit/exception/Exceptions.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/local/QueueStringLogTarget.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/PurgeVisitor.h"

namespace fdb5 {
namespace api {
namespace local {

//----------------------------------------------------------------------------------------------------------------------


PurgeVisitor::PurgeVisitor(eckit::Queue<PurgeElement>& queue, const metkit::mars::MarsRequest& request, bool doit,
                           bool porcelain) :
    QueryVisitor<PurgeElement>(queue, request),
    out_(new QueueStringLogTarget(queue)),
    doit_(doit),
    porcelain_(porcelain) {}

bool PurgeVisitor::visitDatabase(const Catalogue& catalogue) {

    // If the DB is locked for wiping, then it "doesn't exist"
    if (!catalogue.enabled(ControlIdentifier::Wipe)) {
        return false;
    }

    EntryVisitor::visitDatabase(catalogue);

    // If the request is overspecified relative to the DB key, then we
    // bail out here.

    if (!catalogue.key().match(request_)) {
        std::ostringstream ss;
        ss << "Purging not supported for over-specified requests. "
           << "db=" << catalogue.key() << ", request=" << request_;
        throw eckit::UserError(ss.str(), Here());
    }

    ASSERT(!internalVisitor_);
    internalVisitor_.reset(catalogue.purgeVisitor(store()));

    internalVisitor_->visitDatabase(catalogue);

    return true;  // Explore contained indexes
}

bool PurgeVisitor::visitIndex(const Index& index) {
    internalVisitor_->visitIndex(index);

    return true;  // Explore contained entries
}

void PurgeVisitor::visitDatum(const Field& field, const std::string& keyFingerprint) {
    internalVisitor_->visitDatum(field, keyFingerprint);
}

void PurgeVisitor::catalogueComplete(const Catalogue& catalogue) {
    internalVisitor_->catalogueComplete(catalogue);

    internalVisitor_->gatherAuxiliaryURIs();

    if (!porcelain_) {
        internalVisitor_->report(out_);
    }

    if (doit_ || porcelain_) {
        internalVisitor_->purge(out_, porcelain_, doit_);
    }

    // Cleanup

    internalVisitor_.reset();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace local
}  // namespace api
}  // namespace fdb5

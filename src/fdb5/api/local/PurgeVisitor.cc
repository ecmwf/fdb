
/*
 * (C) Copyright 2018- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/local/PurgeVisitor.h"

#include "eckit/exception/Exceptions.h"

#include "fdb5/api/local/QueueStringLogTarget.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/PurgeVisitor.h"
#include "fdb5/LibFdb5.h"

namespace fdb5 {
namespace api {
namespace local {

//----------------------------------------------------------------------------------------------------------------------


PurgeVisitor::PurgeVisitor(eckit::Queue<PurgeElement>& queue,
                           const metkit::MarsRequest& request,
                           bool doit,
                           bool porcelain) :
    QueryVisitor<PurgeElement>(queue, request),
    out_(new QueueStringLogTarget(queue)),
    doit_(doit),
    porcelain_(porcelain) {}

bool PurgeVisitor::visitDatabase(const DB& db) {

    // If the DB is locked for wiping, then it "doesn't exist"
    if (db.wipeLocked()) return false;

    EntryVisitor::visitDatabase(db);

    // If the request is overspecified relative to the DB key, then we
    // bail out here.

    if (!db.key().match(request_)) {
        std::stringstream ss;
        ss << "Purging not supported for over-specified requests. "
           << "db=" << db.key()
           << ", request=" << request_;
        throw eckit::UserError(ss.str(), Here());
    }

    ASSERT(!internalVisitor_);
    internalVisitor_.reset(db.purgeVisitor());

    internalVisitor_->visitDatabase(db);

    return true; // Explore contained indexes
}

bool PurgeVisitor::visitIndex(const Index& index) {
    internalVisitor_->visitIndex(index);

    return true; // Explore contained entries
}

void PurgeVisitor::visitDatum(const Field& field, const std::string& keyFingerprint) {
    internalVisitor_->visitDatum(field, keyFingerprint);
}

void PurgeVisitor::visitDatum(const Field&, const Key&) { NOTIMP; }

void PurgeVisitor::databaseComplete(const DB& db) {
    internalVisitor_->databaseComplete(db);

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

} // namespace local
} // namespace api
} // namespace fdb5

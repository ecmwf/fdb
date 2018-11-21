
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

#include "fdb5/api/local/QueueStringLogTarget.h"

#include "fdb5/database/DB.h"
#include "fdb5/database/PurgeVisitor.h"

namespace fdb5 {
namespace api {
namespace local {

//----------------------------------------------------------------------------------------------------------------------


PurgeVisitor::PurgeVisitor(eckit::Queue<PurgeElement>& queue, bool doit) :
    QueryVisitor(queue),
    out_(new QueueStringLogTarget(queue)),
    doit_(doit) {}

void PurgeVisitor::visitDatabase(const DB& db) {

    EntryVisitor::visitDatabase(db);

    ASSERT(!internalVisitor_);
    internalVisitor_.reset(db.purgeVisitor());

    internalVisitor_->visitDatabase(db);
}

void PurgeVisitor::visitIndex(const Index& index) {
    internalVisitor_->visitIndex(index);
}

void PurgeVisitor::visitDatum(const Field& field, const std::string& keyFingerprint) {
    internalVisitor_->visitDatum(field, keyFingerprint);
}

void PurgeVisitor::visitDatum(const Field&, const Key&) { NOTIMP; }

void PurgeVisitor::databaseComplete(const DB& db) {
    internalVisitor_->databaseComplete(db);

    internalVisitor_->report(out_);

    if (doit_) {
        internalVisitor_->purge(out_);
    }

    // Cleanup

    internalVisitor_.reset();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace local
} // namespace api
} // namespace fdb5

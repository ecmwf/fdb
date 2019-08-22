
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

#include "fdb5/api/local/StatsVisitor.h"

#include "fdb5/database/DB.h"
#include "fdb5/database/StatsReportVisitor.h"

namespace fdb5 {
namespace api {
namespace local {

//----------------------------------------------------------------------------------------------------------------------

bool StatsVisitor::visitDatabase(const DB& db) {

    EntryVisitor::visitDatabase(db);

    ASSERT(!internalVisitor_);
    internalVisitor_.reset(db.statsReportVisitor());

    internalVisitor_->visitDatabase(db);

    return true; // Explore contained indexes
}

bool StatsVisitor::visitIndex(const Index& index) {
    internalVisitor_->visitIndex(index);

    return true; // Explore contained entries
}

void StatsVisitor::visitDatum(const Field& field, const std::string& keyFingerprint) {
    internalVisitor_->visitDatum(field, keyFingerprint);
}

void StatsVisitor::visitDatum(const Field&, const Key&) { NOTIMP; }

void StatsVisitor::databaseComplete(const DB& db) {
    internalVisitor_->databaseComplete(db);

    // Construct the object to push onto the queue

    queue_.emplace(StatsElement { internalVisitor_->indexStatistics(), internalVisitor_->dbStatistics() });

    // Cleanup

    internalVisitor_.reset();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace local
} // namespace api
} // namespace fdb5

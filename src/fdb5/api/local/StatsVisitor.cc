
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

#include "fdb5/database/Catalogue.h"
#include "fdb5/database/StatsReportVisitor.h"

#include <memory>

namespace fdb5 {
namespace api {
namespace local {

//----------------------------------------------------------------------------------------------------------------------

bool StatsVisitor::visitDatabase(const Catalogue& catalogue) {

    EntryVisitor::visitDatabase(catalogue);

    ASSERT(!internalVisitor_);
    internalVisitor_.reset(catalogue.statsReportVisitor());

    internalVisitor_->visitDatabase(catalogue);

    return true;  // Explore contained indexes
}

EntryVisitor::IndexScopePtr StatsVisitor::visitIndex(const Index& index, const Rule& rule,
                                                     eckit::Queue<StatsElement>& /*queue*/) {
    auto inner = internalVisitor_->visitIndex(index, rule);
    if (!inner) {
        return nullptr;  // Skip contained entries
    }

    auto scope = std::make_unique<Scope>(*currentCatalogue_, index, rule);
    scope->inner = std::move(inner);
    return scope;  // Explore contained entries
}

void StatsVisitor::visitDatum(IndexScope& indexScope, const Field& field, const std::string& keyFingerprint) {
    internalVisitor_->visitDatum(*static_cast<Scope&>(indexScope).inner, field, keyFingerprint);
}

void StatsVisitor::catalogueComplete(const Catalogue& catalogue) {
    internalVisitor_->catalogueComplete(catalogue);

    // Construct the object to push onto the queue

    queue_.emplace(StatsElement{internalVisitor_->indexStatistics(), internalVisitor_->dbStatistics()});

    // Cleanup

    internalVisitor_.reset();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace local
}  // namespace api
}  // namespace fdb5

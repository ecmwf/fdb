
// SPDX-FileCopyrightText: 2018- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "fdb5/api/local/StatsVisitor.h"

#include "fdb5/database/Catalogue.h"
#include "fdb5/database/StatsReportVisitor.h"

#include <memory>

namespace fdb5::api::local {

//----------------------------------------------------------------------------------------------------------------------

bool StatsVisitor::visitDatabase(const Catalogue& catalogue) {

    EntryVisitor::visitDatabase(catalogue);

    ASSERT(!internalVisitor_);
    internalVisitor_.reset(catalogue.statsReportVisitor());

    internalVisitor_->visitDatabase(catalogue);

    return true;  // Explore contained indexes
}

EntryVisitor::IndexScopePtr StatsVisitor::visitIndex(const Index& index, OrderedParallelFor::Order& order) {

    auto inner = internalVisitor_->visitIndex(index, order);
    if (!inner) {
        return nullptr;  // Skip contained entries
    }

    auto scope = std::make_unique<Scope>(*currentCatalogue_, index, inner->rule());
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

bool StatsVisitor::supportsConcurrentIndexVisitation() const {
    ASSERT(internalVisitor_);
    return internalVisitor_->supportsConcurrentIndexVisitation();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local

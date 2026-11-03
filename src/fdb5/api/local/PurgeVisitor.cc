
// SPDX-FileCopyrightText: 2018- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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

#include <memory>

namespace fdb5::api::local {

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

EntryVisitor::IndexScopePtr PurgeVisitor::visitIndex(const Index& index, OrderedParallelFor::Order& order) {
    auto inner = internalVisitor_->visitIndex(index, order);
    if (!inner) {
        return nullptr;  // Skip contained entries
    }

    auto scope = std::make_unique<Scope>(*currentCatalogue_, index, inner->rule());
    scope->inner = std::move(inner);
    return scope;  // Explore contained entries
}

void PurgeVisitor::visitDatum(IndexScope& indexScope, const Field& field, const std::string& keyFingerprint) {
    internalVisitor_->visitDatum(*static_cast<Scope&>(indexScope).inner, field, keyFingerprint);
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

bool PurgeVisitor::supportsConcurrentIndexVisitation() const {
    ASSERT(internalVisitor_);
    return internalVisitor_->supportsConcurrentIndexVisitation();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local

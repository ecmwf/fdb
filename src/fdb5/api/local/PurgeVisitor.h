// SPDX-FileCopyrightText: 2018- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date   November 2018

#pragma once

#include "fdb5/api/helpers/PurgeIterator.h"
#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/database/PurgeVisitor.h"
#include "fdb5/database/Store.h"

#include "eckit/filesystem/PathName.h"


namespace fdb5::api::local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class PurgeVisitor : public QueryVisitor<PurgeElement> {
public:

    PurgeVisitor(eckit::Queue<PurgeElement>& queue, const metkit::mars::MarsRequest& request, bool doit,
                 bool porcelain);

    bool visitDatabase(const Catalogue& catalogue) override;
    IndexScopePtr visitIndex(const Index& index, OrderedParallelFor::Order& order) override;
    void catalogueComplete(const Catalogue& catalogue) override;

    // Ensure that overrides of default behaviour can't be used (we've overridden the base using Order)
    IndexScopePtr visitIndex(const Index&, const Rule&) final { NOTIMP; }
    IndexScopePtr visitIndex(const Index&, const Rule&, eckit::Queue<ValueType>& queue) final { NOTIMP; }

    void visitDatum(IndexScope& scope, const Field& field, const std::string& keyFingerprint) override;
    void visitDatum(IndexScope& /*scope*/, const Field& /*field*/, const Key& /*datumKey*/) override { NOTIMP; }

    bool supportsConcurrentIndexVisitation() const override;

private:  // types

    /// Owns the delegate's scope, to hand back on each forwarded visitDatum().
    struct Scope : public IndexScope {
        using IndexScope::IndexScope;
        IndexScopePtr inner;
    };

private:  // members

    eckit::Channel out_;
    bool doit_;
    bool porcelain_;

    std::unique_ptr<fdb5::PurgeVisitor> internalVisitor_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local

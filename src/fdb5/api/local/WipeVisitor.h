// SPDX-FileCopyrightText: 2018- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @author Emanuele Danovaro
/// @date   November 2018

#pragma once

#include <memory>

#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/database/WipeState.h"

namespace fdb5::api::local {

class WipeCatalogueVisitor : public QueryVisitor<CatalogueWipeState> {

public:  // methods

    WipeCatalogueVisitor(eckit::Queue<CatalogueWipeState>& queue, const metkit::mars::MarsRequest& request, bool doit);

    bool visitEntries() override { return false; }

    bool supportsConcurrentIndexVisitation() const final { return false; }

    bool visitDatabase(const Catalogue& catalogue) override;
    IndexScopePtr visitIndex(const Index& index, const Rule& rule, eckit::Queue<CatalogueWipeState>& queue) override;
    void catalogueComplete(const Catalogue& catalogue) override;

    // These methods are not used in the wipe visitor
    void visitDatum(IndexScope& /*scope*/, const Field& /*field*/, const Key& /*datumKey*/) override { NOTIMP; }
    void visitDatum(IndexScope& /*scope*/, const Field& /*field*/, const std::string& /*keyFingerprint*/) override {
        NOTIMP;
    }

    void onDatabaseNotFound(const fdb5::DatabaseNotFoundException& e) override { throw e; }

private:  // members

    bool doit_;

    metkit::mars::MarsRequest indexRequest_;

    std::unique_ptr<CatalogueWipeState> catalogueWipeState_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local

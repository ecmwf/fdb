// SPDX-FileCopyrightText: 2018- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date   November 2018

#pragma once

#include "fdb5/api/helpers/StatusIterator.h"
#include "fdb5/api/local/QueryVisitor.h"


namespace fdb5::api::local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class StatusVisitor : public QueryVisitor<StatusElement> {
public:

    using QueryVisitor<StatusElement>::QueryVisitor;
    bool visitIndexes() override { return false; }
    bool visitEntries() override { return false; }
    bool visitDatabase(const Catalogue& catalogue) override {
        queue_.emplace(catalogue);
        return true;
    }
    IndexScopePtr visitIndex(const Index&, const Rule&, eckit::Queue<ValueType>&) override { NOTIMP; }
    using QueryVisitor<StatusElement>::visitDatum;
    void visitDatum(IndexScope& /*scope*/, const Field&, const Key&) override { NOTIMP; }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local


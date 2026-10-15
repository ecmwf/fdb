// SPDX-FileCopyrightText: 2018- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date   November 2018

#pragma once

#include "fdb5/api/helpers/DumpIterator.h"
#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/api/local/QueueStringLogTarget.h"
#include "fdb5/database/Catalogue.h"

namespace fdb5::api::local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class DumpVisitor : public QueryVisitor<DumpElement> {

public:

    DumpVisitor(eckit::Queue<DumpElement>& queue, const metkit::mars::MarsRequest& request, bool simple) :
        QueryVisitor<DumpElement>(queue, request), out_(new QueueStringLogTarget(queue)), simple_(simple) {}

    bool visitIndexes() override { return false; }
    bool visitEntries() override { return false; }

    bool visitDatabase(const Catalogue& catalogue) override {
        catalogue.dump(out_, simple_);
        return true;
    }

    IndexScopePtr visitIndex(const Index&, const Rule&, eckit::Queue<ValueType>&) override { NOTIMP; }

    using QueryVisitor<DumpElement>::visitDatum;
    void visitDatum(IndexScope& /*scope*/, const Field& /*field*/, const Key& /*datumKey*/) override { NOTIMP; }

private:

    eckit::Channel out_;
    bool simple_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::api::local


// SPDX-FileCopyrightText: 2018- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Simon Smart
/// @date   January 2024

#pragma once

#include "eckit/container/Queue.h"

#include "fdb5/api/helpers/AxesIterator.h"
#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/database/IndexAxis.h"
#include "fdb5/database/Key.h"


namespace fdb5 {

class Index;
class Field;
class Schema;
class Store;
class Catalogue;

namespace api::local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class AxesVisitor : public QueryVisitor<AxesElement> {
public:

    AxesVisitor(eckit::Queue<AxesElement>& queue, const metkit::mars::MarsRequest& request, int level);

    bool visitIndexes() override { return true; }

    bool visitEntries() override { return false; }

    void catalogueComplete(const Catalogue& catalogue) override;

    bool preVisitDatabase(const eckit::URI& uri, const Schema& schema) override;

    bool visitDatabase(const Catalogue& catalogue) override;

    IndexScopePtr visitIndex(const Index& index, const Rule& rule, eckit::Queue<AxesElement>& queue) override;

    using QueryVisitor<AxesElement>::visitDatum;
    void visitDatum(IndexScope& /*scope*/, const Field& /*field*/, const Key& /*key*/) override { NOTIMP; }

private:  // members

    Key dbKey_;
    IndexAxis axes_;
    int level_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace api::local

}  // namespace fdb5

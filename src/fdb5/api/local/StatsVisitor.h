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

/// @author Simon Smart
/// @date   November 2018

#ifndef fdb5_api_local_StatsVisitor_H
#define fdb5_api_local_StatsVisitor_H

#include "fdb5/api/helpers/StatsIterator.h"
#include "fdb5/api/local/QueryVisitor.h"
#include "fdb5/database/StatsReportVisitor.h"
#include "fdb5/database/Store.h"


namespace fdb5 {
namespace api {
namespace local {

/// @note Helper classes for LocalFDB

//----------------------------------------------------------------------------------------------------------------------

class StatsVisitor : public QueryVisitor<StatsElement> {
public:

    using QueryVisitor<StatsElement>::QueryVisitor;

    // We should be able to do this in parallel, but this requires constructing and merging stats
    // objects per-index - which is not yet implemented (just doing accumulation for now)
    // bool supportsConcurrentIndexVisitation() const override { return true; }

    bool visitDatabase(const Catalogue& catalogue) override;
    IndexScopePtr visitIndex(const Index& index, const Rule& rule, eckit::Queue<StatsElement>& queue) override;
    void catalogueComplete(const Catalogue& catalogue) override;

    void visitDatum(IndexScope& scope, const Field& field, const std::string& keyFingerprint) override;
    void visitDatum(IndexScope& /*scope*/, const Field& /*field*/, const Key& /*datumKey*/) override { NOTIMP; }

private:  // types

    /// Owns the delegate's scope, to hand back on each forwarded visitDatum().
    struct Scope : public IndexScope {
        using IndexScope::IndexScope;
        IndexScopePtr inner;
    };

private:  // members

    std::unique_ptr<StatsReportVisitor> internalVisitor_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace local
}  // namespace api
}  // namespace fdb5

#endif

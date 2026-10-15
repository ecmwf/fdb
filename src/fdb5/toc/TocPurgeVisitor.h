// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @author Simon Smart
/// @date   November 2018

#pragma once


#include "fdb5/database/PurgeVisitor.h"
#include "fdb5/database/Store.h"
#include "fdb5/toc/TocStats.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


class TocPurgeVisitor : public PurgeVisitor, public TocStatsReportVisitor {
public:

    TocPurgeVisitor(const TocCatalogue& catalogue, const Store& store);
    ~TocPurgeVisitor() override;

    bool visitDatabase(const Catalogue& catalogue) override;
    void report(std::ostream& out) const override;
    void purge(std::ostream& out, bool porcelain, bool doit) const override;

    void gatherAuxiliaryURIs() override;

    /// @note Inherits concurrent visitor behaviour from TocStatsReportVisitor. All index visitation and ordering
    ///       is done in TocStatsReportVisitor.
    // bool supportsConcurrentIndexVisitation() const override { return true; }

private:  // members

    const Store& store_;
    std::set<eckit::PathName> deleteAuxFiles_;
    std::set<eckit::PathName> keepAuxFiles_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5


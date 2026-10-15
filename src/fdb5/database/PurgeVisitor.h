// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Simon Smart
/// @date   August 2017

#pragma once

#include "fdb5/database/StatsReportVisitor.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class PurgeVisitor : public virtual StatsReportVisitor {

public:  // methods

    using StatsReportVisitor::StatsReportVisitor;

    // n.b. report is only called when either doit=false OR porcelain=false
    virtual void report(std::ostream& out) const = 0;

    virtual void purge(std::ostream& out, bool porcelain, bool doit) const = 0;

    virtual void gatherAuxiliaryURIs() {}  // NOOP by default
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5


// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Simon Smart
/// @date   August 2017

#pragma once

#include "fdb5/database/DbStats.h"
#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/IndexStats.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class StatsReportVisitor : public EntryVisitor {

public:  // methods

    using EntryVisitor::EntryVisitor;

    virtual IndexStats indexStatistics() const = 0;
    virtual DbStats dbStatistics() const = 0;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

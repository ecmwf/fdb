/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   August 2017

#ifndef fdb5_StatsReportVisitor_H
#define fdb5_StatsReportVisitor_H

#include "fdb5/database/DbStats.h"
#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/IndexStats.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class StatsReportVisitor : public EntryVisitor {

public:  // methods

    using EntryVisitor::EntryVisitor;

    virtual IndexStats indexStatistics() const = 0;
    virtual DbStats dbStatistics() const       = 0;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif  // fdb5_StatsReportVisitor_H

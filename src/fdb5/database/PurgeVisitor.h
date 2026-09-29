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

#ifndef fdb5_database_PurgeVisitor_H
#define fdb5_database_PurgeVisitor_H

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

#endif  // fdb5_StatsReportVisitor_H

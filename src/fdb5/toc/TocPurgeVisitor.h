/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @author Simon Smart
/// @date   November 2018

#ifndef fdb5_TocPurgeVisitor_H
#define fdb5_TocPurgeVisitor_H


#include "fdb5/database/PurgeVisitor.h"
#include "fdb5/toc/TocStats.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


class TocPurgeVisitor : public PurgeVisitor, public TocStatsReportVisitor {
public:

    TocPurgeVisitor(const TocDB& db);
    ~TocPurgeVisitor() override;

    void report(std::ostream& out) const override;
    void purge(std::ostream& out, bool verbose) const override;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

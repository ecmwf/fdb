/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   PurgeVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_PurgeVisitor_H
#define fdb5_PurgeVisitor_H


#include "fdb5/database/ReportVisitor.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


class PurgeVisitor : public ReportVisitor {
public:

    PurgeVisitor(DB& db);
    void report(std::ostream &out) const;
    void purge(std::ostream &out) const;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

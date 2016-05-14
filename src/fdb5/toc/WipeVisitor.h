/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   WipeVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_WipeVisitor_H
#define fdb5_WipeVisitor_H


#include "fdb5/toc/ReportVisitor.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


class WipeVisitor : public ReportVisitor {

    mutable std::set<eckit::PathName> files_;
    eckit::Length total_;

    void scan(const eckit::PathName& path);
    const eckit::PathName& mark(const eckit::PathName& path) const;

public:

    WipeVisitor(const eckit::PathName &directory);
    void report(std::ostream &out) const;
    void wipe(std::ostream &out) const;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

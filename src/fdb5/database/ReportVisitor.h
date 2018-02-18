/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   ReportVisitor.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_ReportVisitor_H
#define fdb5_ReportVisitor_H

#include <iosfwd>
#include <set>
#include <map>
#include <vector>

#include "eckit/filesystem/PathName.h"

#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/Report.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


class ReportVisitor : public EntryVisitor {
public:

    ReportVisitor(DB& db);

    ~ReportVisitor();

    fdb5::Report& report() { return report_; }

private: // methods


    virtual void visit(const Index& index,
                       const Field& field,
                       const std::string& indexFingerprint,
                       const std::string& fieldFingerprint);


protected: // members

    DB& db_;

    eckit::PathName directory_;

    std::set<eckit::PathName> activeDataFiles_;
    std::set<eckit::PathName> allDataFiles_;
    std::set<eckit::PathName> allIndexFiles_;
    std::map<eckit::PathName, size_t> indexUsage_;
    std::map<eckit::PathName, size_t> dataUsage_;

    std::set<std::string> active_;

    std::map<Index, IndexStats> indexStats_;

    fdb5::Report report_;

    DbStats dbStats_;
};


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

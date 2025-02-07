/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Report.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_Report_H
#define fdb5_Report_H

#include <map>
#include <set>

#include "eckit/memory/NonCopyable.h"

#include "fdb5/database/DataStats.h"
#include "fdb5/database/DbStats.h"
#include "fdb5/database/IndexStats.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


class Report : private eckit::NonCopyable {
public:

    typedef std::string dbtype_t;

    ~Report();

    void append(const dbtype_t& dbtype, fdb5::DbStats stats);
    void append(const dbtype_t& dbtype, fdb5::IndexStats stats);
    void append(const dbtype_t& dbtype, fdb5::DataStats stats);

    Report& operator+=(const Report& rhs);

private:  // methods

    void print(std::ostream&) const;

    friend std::ostream& operator<<(std::ostream& s, const Report& o) {
        o.print(s);
        return s;
    }

private:  // members

    std::set<dbtype_t> dbtypes_;

    std::map<dbtype_t, fdb5::DbStats> dbStats_;
    std::map<dbtype_t, fdb5::IndexStats> indexStats_;
    std::map<dbtype_t, fdb5::DataStats> dataStats_;
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

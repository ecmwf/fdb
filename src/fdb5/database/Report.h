// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   Report.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#pragma once

#include <map>
#include <set>

#include "fdb5/database/DataStats.h"
#include "fdb5/database/DbStats.h"
#include "fdb5/database/IndexStats.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


class Report {
public:

    typedef std::string dbtype_t;

    Report() = default;

    Report(const Report&) = delete;
    Report& operator=(const Report&) = delete;
    Report(Report&&) = delete;
    Report& operator=(Report&&) = delete;

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

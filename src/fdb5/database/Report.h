/*
 * (C) Copyright 1996-2016 ECMWF.
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

#include "eckit/memory/NonCopyable.h"

#include "fdb5/database/IndexStatistics.h"
#include "fdb5/database/DbStatistics.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


class Report : private eckit::NonCopyable {
public:

    typedef std::string dbtype_t;

    ~Report();

    void append(const dbtype_t& dbtype, DbStatistics* stats);

    Report& operator+= (const Report& rhs);

protected:

    fdb5::IndexStatistics indexStats_;

    std::map<dbtype_t, DbStatistics*> dbStats_;

};


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

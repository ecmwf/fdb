/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"

#include "fdb5/database/Report.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Report::~Report() {
}

void Report::append(const dbtype_t& dbtype, DbStats stats)
{
    std::map<dbtype_t, DbStats>::iterator itr = dbStats_.find(dbtype);
    if(itr != dbStats_.end()) {
        itr->second.add(stats);
    }
    else {
        dbStats_[dbtype] = stats;
    }
}

Report& Report::operator+=(const Report& rhs) {

    // collate DB stats

    for(std::map<dbtype_t, DbStats>::iterator i = dbStats_.begin(); i != dbStats_.end(); ++i) {
        std::map<dbtype_t, DbStats>::const_iterator j = rhs.dbStats_.find(i->first);
        if(j != rhs.dbStats_.end()) {
            i->second.add(j->second);
        }
    }

    // collate Index stats

    for(std::map<dbtype_t, IndexStats>::iterator i = indexStats_.begin(); i != indexStats_.end(); ++i) {
        std::map<dbtype_t, IndexStats>::const_iterator j = rhs.indexStats_.find(i->first);
        if(j != rhs.indexStats_.end()) {
            i->second.add(j->second);
        }
    }

    // collate Data stats

    for(std::map<dbtype_t, DataStats>::iterator i = dataStats_.begin(); i != dataStats_.end(); ++i) {
        std::map<dbtype_t, DataStats>::const_iterator j = rhs.dataStats_.find(i->first);
        if(j != rhs.dataStats_.end()) {
            i->second.add(j->second);
        }
    }

    return *this;
}

void Report::print(std::ostream&) const
{
    NOTIMP;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

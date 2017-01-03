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
    for(std::map<dbtype_t, DbStatistics*>::iterator i = dbStats_.begin(); i != dbStats_.end(); ++i) {
        delete i->second;
    }
}

void Report::append(const dbtype_t& dbtype, DbStatistics* stats)
{
    ASSERT(stats);

    std::map<dbtype_t, DbStatistics*>::iterator itr = dbStats_.find(dbtype);
    if(itr != dbStats_.end()) {
        itr->second->add(*stats);
        delete stats;
    }
    else {
        dbStats_[dbtype] = stats;
    }
}

Report& Report::operator+=(const Report& rhs) {

    indexStats_ += rhs.indexStats_;

    for(std::map<dbtype_t, DbStatistics*>::iterator i = dbStats_.begin(); i != dbStats_.end(); ++i) {
        std::map<dbtype_t, DbStatistics*>::const_iterator j = rhs.dbStats_.find(i->first);
        if(j != rhs.dbStats_.end()) {
            i->second->add(*(j->second));
        }
    }

    return *this;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

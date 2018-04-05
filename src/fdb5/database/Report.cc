/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/log/Statistics.h"
#include "eckit/log/Log.h"
#include "fdb5/LibFdb.h"

#include "fdb5/database/Report.h"

using eckit::Statistics;
using eckit::Log;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Report::~Report() {
}

void Report::append(const dbtype_t& dbtype, DbStats stats)
{
    dbtypes_.insert(dbtype);

    std::map<dbtype_t, DbStats>::iterator itr = dbStats_.find(dbtype);
    if(itr != dbStats_.end()) {
        itr->second.add(stats);
    }
    else {
        dbStats_[dbtype] = stats;
    }
}

Report& Report::operator+=(const Report& rhs) {

    Log::debug<LibFdb>() << "Collating reports" << std::endl;

    // union of dbtypes

    std::set<dbtype_t> join;
    std::set_union(dbtypes_.begin(),
                   dbtypes_.end(),
                   rhs.dbtypes_.begin(),
                   rhs.dbtypes_.end(),
                   std::insert_iterator< std::set<dbtype_t> >(join, join.begin()));

    std::swap(dbtypes_, join);

    // collate DB stats

    for(std::map<dbtype_t, DbStats>::const_iterator i = rhs.dbStats_.begin(); i != rhs.dbStats_.end(); ++i) {
        Log::debug<LibFdb>() << "dbtype " << i->first << std::endl;
        std::map<dbtype_t, DbStats>::iterator j = dbStats_.find(i->first);
        if(j != dbStats_.end()) {
            j->second.add(i->second);
        }
        else{
            dbStats_[i->first] = i->second;
        }
    }

    // collate Index stats

    for(std::map<dbtype_t, IndexStats>::const_iterator i = rhs.indexStats_.begin(); i != rhs.indexStats_.end(); ++i) {
        Log::debug<LibFdb>() << "dbtype " << i->first << std::endl;
        std::map<dbtype_t, IndexStats>::iterator j = indexStats_.find(i->first);
        if(j != indexStats_.end()) {
            j->second.add(i->second);
        }
        else{
            indexStats_[i->first] = i->second;
        }
    }

    // collate Data stats

    for(std::map<dbtype_t, DataStats>::const_iterator i = rhs.dataStats_.begin(); i != rhs.dataStats_.end(); ++i) {
        Log::debug<LibFdb>() << "dbtype " << i->first << std::endl;
        std::map<dbtype_t, DataStats>::iterator j = dataStats_.find(i->first);
        if(j != dataStats_.end()) {
            j->second.add(i->second);
        }
        else{
            dataStats_[i->first] = i->second;
        }
    }

    return *this;
}

void Report::print(std::ostream& out) const
{
    const char* sep = "";
    for(std::set<dbtype_t>::const_iterator i = dbtypes_.begin(); i != dbtypes_.end(); ++i) {

        dbtype_t dbtype = *i;

        out << sep << "Database Type \'" << dbtype << "\'" << std::endl;

        std::map<dbtype_t, DbStats>::const_iterator db = dbStats_.find(dbtype);
        if(db != dbStats_.end())
            db->second.report(out);

        std::map<dbtype_t, IndexStats>::const_iterator idx = indexStats_.find(dbtype);
        if(idx != indexStats_.end())
            idx->second.report(out);

        std::map<dbtype_t, DataStats>::const_iterator data = dataStats_.find(dbtype);
        if(data != dataStats_.end())
            data->second.report(out);

        sep = "\n";
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

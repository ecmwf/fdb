/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>

#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"
#include "eckit/log/Statistics.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Report.h"

using eckit::Log;
using eckit::Statistics;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Report::~Report() {}

void Report::append(const dbtype_t& dbType, DbStats stats) {
    dbtypes_.insert(dbType);

    std::map<dbtype_t, DbStats>::iterator itr = dbStats_.find(dbType);
    if (itr != dbStats_.end()) {
        itr->second.add(stats);
    }
    else {
        dbStats_[dbType] = stats;
    }
}

Report& Report::operator+=(const Report& rhs) {

    LOG_DEBUG_LIB(LibFdb5) << "Collating reports" << std::endl;

    // union of dbtypes

    std::set<dbtype_t> join;
    std::set_union(dbtypes_.begin(), dbtypes_.end(), rhs.dbtypes_.begin(), rhs.dbtypes_.end(),
                   std::insert_iterator<std::set<dbtype_t>>(join, join.begin()));

    std::swap(dbtypes_, join);

    // collate DB stats

    for (const auto& [dbType, dbStat] : rhs.dbStats_) {
        LOG_DEBUG_LIB(LibFdb5) << "dbtype " << dbType << std::endl;
        std::map<dbtype_t, DbStats>::iterator j = dbStats_.find(dbType);
        if (j != dbStats_.end()) {
            j->second.add(dbStat);
        }
        else {
            dbStats_[dbType] = dbStat;
        }
    }

    // collate Index stats

    for (const auto& [dbType, indexStat] : rhs.indexStats_) {
        LOG_DEBUG_LIB(LibFdb5) << "dbtype " << dbType << std::endl;
        std::map<dbtype_t, IndexStats>::iterator j = indexStats_.find(dbType);
        if (j != indexStats_.end()) {
            j->second.add(indexStat);
        }
        else {
            indexStats_[dbType] = indexStat;
        }
    }

    // collate Data stats

    for (const auto& [dbType, dataStat] : rhs.dataStats_) {
        LOG_DEBUG_LIB(LibFdb5) << "dbtype " << dbType << std::endl;
        std::map<dbtype_t, DataStats>::iterator j = dataStats_.find(dbType);
        if (j != dataStats_.end()) {
            j->second.add(dataStat);
        }
        else {
            dataStats_[dbType] = dataStat;
        }
    }

    return *this;
}

void Report::print(std::ostream& out) const {
    const char* sep = "";
    for (std::set<dbtype_t>::const_iterator i = dbtypes_.begin(); i != dbtypes_.end(); ++i) {

        dbtype_t dbType = *i;

        out << sep << "Database Type \'" << dbType << "\'" << std::endl;

        std::map<dbtype_t, DbStats>::const_iterator db = dbStats_.find(dbType);
        if (db != dbStats_.end()) {
            db->second.report(out);
        }

        std::map<dbtype_t, IndexStats>::const_iterator idx = indexStats_.find(dbType);
        if (idx != indexStats_.end()) {
            idx->second.report(out);
        }

        std::map<dbtype_t, DataStats>::const_iterator data = dataStats_.find(dbType);
        if (data != dataStats_.end()) {
            data->second.report(out);
        }

        sep = "\n";
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

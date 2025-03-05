/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "fdb5/api/helpers/StatsIterator.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

StatsElement::StatsElement(const IndexStats& iStats, const DbStats& dbStats) :
    indexStatistics(iStats), dbStatistics(dbStats) {}


StatsElement::StatsElement(eckit::Stream& s) :
    indexStatistics(eckit::Reanimator<IndexStatsContent>::reanimate(s)),
    dbStatistics(eckit::Reanimator<DbStatsContent>::reanimate(s)) {}


void StatsElement::encode(eckit::Stream& s) const {
    s << indexStatistics;
    s << dbStatistics;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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

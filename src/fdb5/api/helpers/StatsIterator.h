// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Simon Smart
/// @date   November 2018

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#pragma once

#include <string>

#include "eckit/filesystem/PathName.h"

#include "fdb5/api/helpers/APIIterator.h"
#include "fdb5/database/DbStats.h"
#include "fdb5/database/Index.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

struct StatsElement {

    StatsElement() = default;
    StatsElement(const IndexStats& iStats, const DbStats& dbStats);
    StatsElement(eckit::Stream& s);

    IndexStats indexStatistics;
    DbStats dbStatistics;

private:  // methods

    void encode(eckit::Stream& s) const;

    friend eckit::Stream& operator<<(eckit::Stream& s, const StatsElement& r) {
        r.encode(s);
        return s;
    }
};

using StatsIterator = APIIterator<StatsElement>;

using StatsAggregateIterator = APIAggregateIterator<StatsElement>;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5


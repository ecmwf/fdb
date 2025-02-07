/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   November 2018

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#ifndef fdb5_api_StatsIterator_H
#define fdb5_api_StatsIterator_H

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

using StatsAsyncIterator = APIAsyncIterator<StatsElement>;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include "fdb5/database/DbStats.h"

#include "eckit/serialisation/Reanimator.h"
#include "eckit/serialisation/Stream.h"

#include <cstddef>
#include <ostream>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

// Minimal DB-level statistics for the RADOS backend: databases, indexes and fields visited.
// Byte totals are intentionally omitted; adding them would require per-object HEAD reads.
class RadosDbStats : public DbStatsContent {
public:

    RadosDbStats();
    RadosDbStats(eckit::Stream&);

    static DbStats make() { return DbStats(new RadosDbStats()); }

    size_t dbCount_;
    size_t indexCount_;
    size_t fieldCount_;

    RadosDbStats& operator+=(const RadosDbStats& rhs);

    void add(const DbStatsContent&) override;
    void report(std::ostream& out, const char* indent) const override;

public:  // For Streamable

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

protected:  // For Streamable

    void encode(eckit::Stream&) const override;
    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<RadosDbStats> reanimator_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

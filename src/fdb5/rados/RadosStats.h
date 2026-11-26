// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Metin Cakircali
/// @date   Aug 2026

#pragma once

#include "fdb5/database/DbStats.h"

#include "eckit/log/JSON.h"
#include "eckit/serialisation/Reanimator.h"
#include "eckit/serialisation/Stream.h"

#include <cstddef>
#include <ostream>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

// Byte totals are intentionally omitted; adding them would require per-object HEAD reads.
class RadosDbStats : public DbStatsContent {
public:

    RadosDbStats();
    RadosDbStats(eckit::Stream&);

    size_t dbCount_;
    size_t indexCount_;
    size_t fieldCount_;

    RadosDbStats& operator+=(const RadosDbStats& rhs);

    void add(const DbStatsContent& rhs) override;

    void report(std::ostream& out, const char* indent) const override;
    void json(eckit::JSON& json) const override;

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

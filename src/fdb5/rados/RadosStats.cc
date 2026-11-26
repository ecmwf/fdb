// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/rados/RadosStats.h"

#include "fdb5/database/DbStats.h"

#include "eckit/log/JSON.h"
#include "eckit/serialisation/Reanimator.h"
#include "eckit/serialisation/Stream.h"

#include <ostream>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

::eckit::ClassSpec RadosDbStats::classSpec_ = {
    &DbStatsContent::classSpec(),
    "RadosDbStats",
};
::eckit::Reanimator<RadosDbStats> RadosDbStats::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

RadosDbStats::RadosDbStats() : dbCount_(0), indexCount_(0), fieldCount_(0) {}

RadosDbStats::RadosDbStats(eckit::Stream& out) {
    out >> dbCount_;
    out >> indexCount_;
    out >> fieldCount_;
}

RadosDbStats& RadosDbStats::operator+=(const RadosDbStats& rhs) {
    dbCount_ += rhs.dbCount_;
    indexCount_ += rhs.indexCount_;
    fieldCount_ += rhs.fieldCount_;
    return *this;
}

void RadosDbStats::add(const DbStatsContent& rhs) {
    *this += dynamic_cast<const RadosDbStats&>(rhs);
}

void RadosDbStats::report(std::ostream& out, const char* indent) const {
    reportCount(out, "Databases", dbCount_, indent);
    reportCount(out, "Indexes", indexCount_, indent);
    reportCount(out, "Fields", fieldCount_, indent);
}

void RadosDbStats::json(eckit::JSON& json) const {
    json << "databases" << dbCount_;
    json << "indexes" << indexCount_;
    json << "fields" << fieldCount_;
}

void RadosDbStats::encode(eckit::Stream& out) const {
    out << dbCount_;
    out << indexCount_;
    out << fieldCount_;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

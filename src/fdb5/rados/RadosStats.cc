/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/rados/RadosStats.h"

#include "fdb5/database/DbStats.h"

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

void RadosDbStats::encode(eckit::Stream& out) const {
    out << dbCount_;
    out << indexCount_;
    out << fieldCount_;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/pmem/PMemStatistics.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

PMemStatistics::PMemStatistics():
    poolsSize_(0),
    schemaSize_(0),
    poolsCount_(0),
    indexesCount_(0)
{
}

PMemStatistics &PMemStatistics::operator+=(const PMemStatistics &rhs) {
    poolsSize_ += rhs.poolsSize_;
    schemaSize_ += rhs.schemaSize_;
    poolsCount_ += rhs.poolsCount_;
    indexesCount_ += rhs.indexesCount_;
    return *this;
}

void PMemStatistics::add(const DbStatistics& rhs)
{
    const PMemStatistics& stats = dynamic_cast<const PMemStatistics&>(rhs);
    *this += stats;
}

void PMemStatistics::report(std::ostream &out, const char *indent) const {
    reportCount(out, "Pools ", poolsCount_, indent);
    reportBytes(out, "Size of pools", poolsSize_, indent);
    reportCount(out, "Indexes", indexesCount_, indent);
    reportBytes(out, "Size of schemas", schemaSize_, indent);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

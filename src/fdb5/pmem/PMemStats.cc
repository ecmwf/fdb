/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/pmem/PMemStats.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


PMemDbStats::PMemDbStats():
    dataPoolsSize_(0),
    indexPoolsSize_(0),
    schemaSize_(0),
    dataPoolsCount_(0),
    indexPoolsCount_(0),
    indexesCount_(0) {}


PMemDbStats& PMemDbStats::operator+=(const PMemDbStats &rhs) {
    dataPoolsSize_ += rhs.dataPoolsSize_;
    indexPoolsSize_ += rhs.indexPoolsSize_;
    schemaSize_ += rhs.schemaSize_;
    dataPoolsCount_ += rhs.dataPoolsCount_;
    indexPoolsCount_ += rhs.indexPoolsCount_;
    indexesCount_ += rhs.indexesCount_;
    return *this;
}


void PMemDbStats::add(const DbStatsContent& rhs)
{
    const PMemDbStats& stats = dynamic_cast<const PMemDbStats&>(rhs);
    *this += stats;
}

void PMemDbStats::report(std::ostream &out, const char *indent) const {
    reportCount(out, "Data Pools ", dataPoolsCount_, indent);
    reportBytes(out, "Size of data pools", dataPoolsSize_, indent);
    reportCount(out, "Index Pools ", indexPoolsCount_, indent);
    reportBytes(out, "Size of index pools", indexPoolsSize_, indent);
    reportCount(out, "Indexes", indexesCount_, indent);
    reportBytes(out, "Size of schemas", schemaSize_, indent);
}


//----------------------------------------------------------------------------------------------------------------------


PMemIndexStats::PMemIndexStats():
    fieldsCount_(0),
    duplicatesCount_(0),
    fieldsSize_(0),
    duplicatesSize_(0) {}


PMemIndexStats &PMemIndexStats::operator+=(const PMemIndexStats &rhs) {
    fieldsCount_ += rhs.fieldsCount_;
    duplicatesCount_ += rhs.duplicatesCount_;
    fieldsSize_ += rhs.fieldsSize_;
    duplicatesSize_ += rhs.duplicatesSize_;

    return *this;
}

void PMemIndexStats::add(const IndexStatsContent& rhs)
{
    const PMemIndexStats& stats = dynamic_cast<const PMemIndexStats&>(rhs);
    *this += stats;
}

void PMemIndexStats::report(std::ostream &out, const char *indent) const {
    reportCount(out, "Fields", fieldsCount_, indent);
    reportBytes(out, "Size of fields", fieldsSize_, indent);
    reportCount(out, "Duplicated fields ", duplicatesCount_, indent);
    reportBytes(out, "Size of duplicates", duplicatesSize_, indent);
    reportCount(out, "Reacheable fields ", fieldsCount_ - duplicatesCount_, indent);
    reportBytes(out, "Reachable size", fieldsSize_ - duplicatesSize_, indent);
}


//----------------------------------------------------------------------------------------------------------------------


PMemDataStats::PMemDataStats() {
}


PMemDataStats &PMemDataStats::operator+=(const PMemDataStats &rhs) {

    NOTIMP;

    return *this;
}

void PMemDataStats::add(const DataStatsContent& rhs)
{
    const PMemDataStats& stats = dynamic_cast<const PMemDataStats&>(rhs);
    *this += stats;
}

void PMemDataStats::report(std::ostream &out, const char *indent) const {
    NOTIMP;
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

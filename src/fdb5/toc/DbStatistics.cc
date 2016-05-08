/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/DbStatistics.h"

// #include "eckit/log/Bytes.h"
// #include "eckit/log/Plural.h"

// #include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/TocHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DbStatistics::DbStatistics():
    tocRecords_(0),
    tocSize_(0),
    schemaSize_(0),
    totalDataFiles_(0),
    totalAdoptedFiles_(0),
    totalIndexFiles_(0) {}


void DbStatistics::update(TocHandler& handler) {
    tocRecords_ += handler.numberOfRecords();
    tocSize_ += handler.tocPath().size();
    schemaSize_ += handler.schemaPath().size();
}



DbStatistics &DbStatistics::operator+=(const DbStatistics &rhs) {
    tocRecords_ += rhs.tocRecords_;
    tocSize_ += rhs.tocSize_;
    schemaSize_ += rhs.schemaSize_;
    totalDataFiles_ += rhs.totalDataFiles_;
    totalAdoptedFiles_ += rhs.totalAdoptedFiles_;
    totalIndexFiles_ += rhs.totalIndexFiles_;

    return *this;
}


void DbStatistics::report(std::ostream &out, const char *indent) const {
    reportCount(out, "TOC records", tocRecords_, indent);
    reportBytes(out, "Size of TOC files", tocSize_, indent);
    reportBytes(out, "Size of schemas files", schemaSize_, indent);
    reportBytes(out, "Size of data files", totalDataFiles_, indent);
    reportBytes(out, "Size of adopted files", totalAdoptedFiles_, indent);
    reportBytes(out, "Size of index files", totalIndexFiles_, indent);
    reportBytes(out, "Total size", tocSize_, indent);
    reportBytes(out, "Size of TOC files", tocSize_ + schemaSize_ +  totalIndexFiles_ + totalDataFiles_, indent);
}

//----------------------------------------------------------------------------------------------------------------------

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

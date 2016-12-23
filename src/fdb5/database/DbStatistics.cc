/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/DbStatistics.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DbStatistics::DbStatistics():
    tocRecordsCount_(0),
    tocFileSize_(0),
    schemaFileSize_(0),
    ownedFilesSize_(0),
    adoptedFilesSize_(0),
    indexFilesSize_(0),
    ownedFilesCount_(0),
    adoptedFilesCount_(0),
    indexFilesCount_(0)
{
}

DbStatistics &DbStatistics::operator+=(const DbStatistics &rhs) {
    tocRecordsCount_ += rhs.tocRecordsCount_;
    tocFileSize_ += rhs.tocFileSize_;
    schemaFileSize_ += rhs.schemaFileSize_;
    ownedFilesSize_ += rhs.ownedFilesSize_;
    adoptedFilesSize_ += rhs.adoptedFilesSize_;
    indexFilesSize_ += rhs.indexFilesSize_;
    ownedFilesCount_ += rhs.ownedFilesCount_;
    adoptedFilesCount_ += rhs.adoptedFilesCount_;
    indexFilesCount_ += rhs.indexFilesCount_;

    return *this;
}


void DbStatistics::report(std::ostream &out, const char *indent) const {
    reportCount(out, "TOC records", tocRecordsCount_, indent);
    reportBytes(out, "Size of TOC files", tocFileSize_, indent);
    reportBytes(out, "Size of schemas files", schemaFileSize_, indent);
    reportCount(out, "TOC records", tocRecordsCount_, indent);

    reportCount(out, "Owned data files", ownedFilesCount_, indent);
    reportBytes(out, "Size of owned data files", ownedFilesSize_, indent);
    reportCount(out, "Adopted data files", adoptedFilesCount_, indent);

    reportBytes(out, "Size of adopted data files", adoptedFilesSize_, indent);
    reportCount(out, "Index files", indexFilesCount_, indent);

    reportBytes(out, "Size of index files", indexFilesSize_, indent);
    reportBytes(out, "Size of TOC files", tocFileSize_, indent);
    reportBytes(out, "Total owned size", tocFileSize_ + schemaFileSize_ +  indexFilesSize_ + ownedFilesSize_, indent);
    reportBytes(out, "Total size", tocFileSize_ + schemaFileSize_ +  indexFilesSize_ + ownedFilesSize_ + adoptedFilesSize_, indent);

}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

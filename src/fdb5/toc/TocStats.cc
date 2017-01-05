/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>

#include "eckit/log/Log.h"
#include "fdb5/LibFdb.h"

#include "fdb5/toc/TocStats.h"

using eckit::Log;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocDbStats::TocDbStats():
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
    Log::debug<LibFdb>() << Here() << std::endl;
}

TocDbStats& TocDbStats::operator+=(const TocDbStats &rhs) {

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

void TocDbStats::add(const DbStatsContent& rhs)
{
    std::ostream& out = Log::debug<LibFdb>();

    out << Here() <<  std::endl;
    rhs.report(out, "");

    const TocDbStats& stats = dynamic_cast<const TocDbStats&>(rhs);
    *this += stats;
}

void TocDbStats::report(std::ostream &out, const char *indent) const {

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

TocIndexStats::TocIndexStats():
    fieldsCount_(0),
    duplicatesCount_(0),
    fieldsSize_(0),
    duplicatesSize_(0) {}


TocIndexStats &TocIndexStats::operator+=(const TocIndexStats &rhs) {
    fieldsCount_ += rhs.fieldsCount_;
    duplicatesCount_ += rhs.duplicatesCount_;
    fieldsSize_ += rhs.fieldsSize_;
    duplicatesSize_ += rhs.duplicatesSize_;

    return *this;
}

void TocIndexStats::add(const IndexStatsContent& rhs)
{
    const TocIndexStats& stats = dynamic_cast<const TocIndexStats&>(rhs);
    *this += stats;
}

void TocIndexStats::report(std::ostream &out, const char *indent) const {
    reportCount(out, "Fields", fieldsCount_, indent);
    reportBytes(out, "Size of fields", fieldsSize_, indent);
    reportCount(out, "Duplicated fields ", duplicatesCount_, indent);
    reportBytes(out, "Size of duplicates", duplicatesSize_, indent);
    reportCount(out, "Reacheable fields ", fieldsCount_ - duplicatesCount_, indent);
    reportBytes(out, "Reachable size", fieldsSize_ - duplicatesSize_, indent);
}

//----------------------------------------------------------------------------------------------------------------------

TocDataStats::TocDataStats() {
}

TocDataStats& TocDataStats::operator+=(const TocDataStats& rhs) {

    std::set<eckit::PathName> intersect;
    std::set_union(allDataFiles_.begin(),
                   allDataFiles_.end(),
                   rhs.allDataFiles_.begin(),
                   rhs.allDataFiles_.end(),
                   std::insert_iterator< std::set<eckit::PathName> >(intersect, intersect.begin()));

    std::swap(allDataFiles_, intersect);

    intersect.clear();
    std::set_union(activeDataFiles_.begin(),
                   activeDataFiles_.end(),
                   rhs.activeDataFiles_.begin(),
                   rhs.activeDataFiles_.end(),
                   std::insert_iterator< std::set<eckit::PathName> >(intersect, intersect.begin()));

    std::swap(activeDataFiles_, intersect);

    for(std::map<eckit::PathName, size_t>::const_iterator i = rhs.dataUsage_.begin(); i != rhs.dataUsage_.end(); ++i) {
        dataUsage_[i->first] += i->second;
    }

    return *this;
}

void TocDataStats::add(const IndexStatsContent& rhs)
{
    const TocDataStats& stats = dynamic_cast<const TocDataStats&>(rhs);
    *this += stats;
}

void TocDataStats::report(std::ostream &out, const char *indent) const {
    NOTIMP;
}

//----------------------------------------------------------------------------------------------------------------------


} // namespace fdb5

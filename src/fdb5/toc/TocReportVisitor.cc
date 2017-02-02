/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/TocReportVisitor.h"

#include "eckit/log/Bytes.h"
#include "eckit/log/Plural.h"

#include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/TocHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocReportVisitor::TocReportVisitor(TocDB& db) :
    db_(db) {
}

void TocReportVisitor::visit(const Index& index,
                   const Field& field,
                   const std::string& indexFingerprint,
                   const std::string& fieldFingerprint) {

    IndexStats& stats = indexStats_[&index];

    const eckit::PathName& directory = db_.directory();

    stats.addFieldsCount(1);
    stats.addFieldsSize( field.location().length() );

    const eckit::PathName &dataPath  = field.location().url();
    const eckit::PathName &indexPath = index.location().url();


    TocDbStats* s = new TocDbStats();

    if (allDataFiles_.find(dataPath) == allDataFiles_.end()) {
        if (dataPath.dirName().sameAs(directory)) {
            (*s).ownedFilesSize_ += dataPath.size();
            (*s).ownedFilesCount_++;

        } else {
            (*s).adoptedFilesSize_ += dataPath.size();
            (*s).adoptedFilesCount_++;

        }
        allDataFiles_.insert(dataPath);
    }

    if (allIndexFiles_.find(indexPath) == allIndexFiles_.end()) {
        (*s).indexFilesSize_ += indexPath.size();
        allIndexFiles_.insert(indexPath);
        (*s).indexFilesCount_++;
    }

    dbStats_ += DbStats(s);

    indexUsage_[indexPath]++;
    dataUsage_[dataPath]++;

    std::string unique = indexFingerprint + "+" + fieldFingerprint;

    if (active_.find(unique) == active_.end()) {
        active_.insert(unique);
        activeDataFiles_.insert(dataPath);
    } else {
        stats.addDuplicatesCount(1);
        stats.addDuplicatesSize(field.location().length());
        indexUsage_[indexPath]--;
        dataUsage_[dataPath]--;
    }
}

//DbStats TocReportVisitor::dbStatistics() const {
//    return dbStats_;
//}

// IndexStats TocReportVisitor::indexStatistics() const {
//     TocIndexStats total;
//     for (std::map<const Index *, TocIndexStats>::const_iterator i = indexStats_.begin(); i != indexStats_.end(); ++i) {
//         total += i->second;
//     }
//     return total;
// }

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

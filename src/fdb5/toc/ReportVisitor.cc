/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/ReportVisitor.h"

#include "eckit/log/Bytes.h"
#include "eckit/log/Plural.h"

#include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/TocHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------



//----------------------------------------------------------------------------------------------------------------------

ReportVisitor::ReportVisitor(const eckit::PathName &directory) :
    directory_(directory) {

    TocHandler handler(directory_);
    dbStats_.update(handler);
}

ReportVisitor::~ReportVisitor() {
}

void ReportVisitor::visit(const Index &index,
                          const std::string &indexFingerprint,
                          const std::string &fieldFingerprint,
                          const Field &field) {

    IndexStatistics &stats = indexStats_[&index];

    ++stats.fieldsCount_;
    stats.fieldsSize_ += field.length();

    const eckit::PathName &dataPath = field.path();
    const eckit::PathName &indexPath = index.path();

    if (allDataFiles_.find(dataPath) == allDataFiles_.end()) {
        if (dataPath.dirName().sameAs(directory_)) {
            dbStats_.ownedFilesSize_ += dataPath.size();
            dbStats_.ownedFilesCount_++;

        } else {
            dbStats_.adoptedFilesSize_ += dataPath.size();
            dbStats_.adoptedFilesCount_++;

        }
        allDataFiles_.insert(dataPath);
    }

    if (allIndexFiles_.find(indexPath) == allIndexFiles_.end()) {
        dbStats_.indexFilesSize_ += indexPath.size();
        allIndexFiles_.insert(indexPath);
        dbStats_.indexFilesCount_++;
    }


    indexUsage_[indexPath]++;
    dataUsage_[dataPath]++;

    std::string unique = indexFingerprint + "+" + fieldFingerprint;

    if (active_.find(unique) == active_.end()) {
        active_.insert(unique);
        activeDataFiles_.insert(dataPath);
    } else {
        ++stats.duplicatesCount_;
        stats.duplicatesSize_ += field.length();
        indexUsage_[indexPath]--;
        dataUsage_[dataPath]--;
    }
}

DbStatistics ReportVisitor::dbStatistics() const {
    return dbStats_;
}

IndexStatistics ReportVisitor::indexStatistics() const {
    IndexStatistics total;
    for (std::map<const Index *, IndexStatistics>::const_iterator i = indexStats_.begin(); i != indexStats_.end(); ++i) {
        total += i->second;
    }
    return total;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

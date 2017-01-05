/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/ReportVisitor.h"

#include "eckit/log/Bytes.h"
#include "eckit/log/Plural.h"

#include "fdb5/database/IndexLocation.h"

namespace fdb5 {


//----------------------------------------------------------------------------------------------------------------------

#if 0
class ReportLocationVisitor : public FieldLocationVisitor {

public:

    ReportLocationVisitor(const eckit::PathName& directory,
                          DbStats& dbStats,
                          IndexStats& idxStats,
                          std::set<eckit::PathName>& allDataFiles,
                          std::set<eckit::PathName>& activeDataFiles,
                          std::map<eckit::PathName, size_t>& dataUsage,
                          bool unique) :
        directory_(directory),
        dbStats_(dbStats),
        idxStats_(idxStats),
        allDataFiles_(allDataFiles),
        activeDataFiles_(activeDataFiles),
        dataUsage_(dataUsage),
        unique_(unique) {}

    virtual void operator() (const FieldLocation& floc) {
        common(floc.length(), floc.url());
    }

private:

    void common(size_t length, const eckit::PathName& path) {

        ++idxStats_.fieldsCount_;
        idxStats_.fieldsSize_ += length;

        if (allDataFiles_.find(path) == allDataFiles_.end()) {
            if (path.dirName().sameAs(directory_)) {
                dbStats_.ownedFilesSize_ += path.size();
                dbStats_.ownedFilesCount_++;

            } else {
                dbStats_.adoptedFilesSize_ += path.size();
                dbStats_.adoptedFilesCount_++;

            }

            allDataFiles_.insert(path);
        }

        if (unique_) {
            activeDataFiles_.insert(path);
            dataUsage_[path]++;
        } else {
            idxStats_.duplicatesSize_ += length;
            ++idxStats_.duplicatesCount_;
        }
    }

    const eckit::PathName& directory_;
    DbStats& dbStats_;
    IndexStats& idxStats_;
    bool unique_;
};

//----------------------------------------------------------------------------------------------------------------------

class ReportIndexVisitor : public IndexLocationVisitor {

public:

    ReportIndexVisitor(DbStats& dbStats,
                       IndexStats& idxStats,
                       std::set<eckit::PathName>& allIndexFiles,
                       std::map<eckit::PathName, size_t>& indexUsage,
                       bool unique) :
        dbStats_(dbStats),
//        idxStats_(idxStats),
        allIndexFiles_(allIndexFiles),
        indexUsage_(indexUsage),
        unique_(unique)
    {
    }

    virtual void operator() (const IndexLocation& idxloc) {

        const eckit::PathName path = idxloc.url();

        if (allIndexFiles_.find(path) == allIndexFiles_.end()) {
            dbStats_.indexFilesSize_ += path.size();
            allIndexFiles_.insert(path);
            dbStats_.indexFilesCount_++;
        }

        if (unique_) {
            indexUsage_[path]++;
        }
    }

private:

    DbStats& dbStats_;
//    IndexStats& idxStats_;
    std::set<eckit::PathName>& allIndexFiles_;
    std::map<eckit::PathName, size_t>& indexUsage_;
    bool unique_;
};
#endif

//----------------------------------------------------------------------------------------------------------------------

ReportVisitor::ReportVisitor(DB& db) :
    db_(db),
    directory_(db.basePath()) {

    report_.append(db.dbType(), db.statistics());
}

ReportVisitor::~ReportVisitor() {
}

void ReportVisitor::visit(const Index& index,
                          const Field& field,
                          const std::string& indexFingerprint,
                          const std::string& fieldFingerprint) {

    std::string unique_id = indexFingerprint + "+" + fieldFingerprint;

    bool unique = (active_.find(unique_id) == active_.end());
    if (unique)
        active_.insert(unique_id);

#if 0
    ReportLocationVisitor locVisitor(directory_, report_, allDataFiles_, activeDataFiles_, dataUsage_, unique);
    field.location().visit(locVisitor);

    ReportIndexVisitor idxVisitor(dbStats_, stats, allIndexFiles_, indexUsage_, unique);
    index.visit(idxVisitor);
#endif
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

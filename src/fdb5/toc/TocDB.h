/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TocDB.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_TocDB_H
#define fdb5_TocDB_H

#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/toc/TocEngine.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class TocDB : public DB, public TocHandler {

public: // methods

    TocDB(const Key& key, const fdb5::Config& config);
    TocDB(const eckit::PathName& directory, const fdb5::Config& config);

    ~TocDB() override;

    static const char* dbTypeName() { return TocEngine::typeName(); }

protected: // methods

    std::string dbType() const override;

    void checkUID() const override;
    bool open() override;
    void close() override;
    void flush() override;
    bool exists() const override;
    void visitEntries(EntryVisitor& visitor, bool sorted=false) override;
    void visit(DBVisitor& visitor) override;
    void dump(std::ostream& out, bool simple=false) const override;
    std::string owner() const override;
    const eckit::PathName& basePath() const override;
    std::vector<eckit::PathName> metadataPaths() const override;
    const Schema& schema() const override;

    eckit::DataHandle *retrieve(const Key &key) const override;
    void archive(const Key &key, const void *data, eckit::Length length) override;
    void axis(const std::string &keyword, eckit::StringSet &s) const override;

    StatsReportVisitor* statsReportVisitor() const override;
    PurgeVisitor* purgeVisitor() const override;
    WipeVisitor* wipeVisitor(const metkit::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const override;
    void maskIndexEntry(const Index& index) const override;

    void loadSchema();

    DbStats statistics() const override;

    std::vector<Index> indexes(bool sorted=false) const override;

    void allMasked(std::set<std::pair<eckit::PathName, eckit::Offset>>& metadata,
                   std::set<eckit::PathName>& data) const override;

    // Control access properties of the DB

    void control(const ControlAction& action, const ControlIdentifiers& identifiers) const override;

    bool retrieveLocked() const override;
    bool archiveLocked() const override;
    bool listLocked() const override;
    bool wipeLocked() const override;

private: // members

    friend class TocWipeVisitor;

    Schema schema_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

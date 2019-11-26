/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TocCatalogue.h
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

class TocCatalogue : public Catalogue, public TocHandler {

public: // methods

    TocCatalogue(const Key& key, const fdb5::Config& config);
    TocCatalogue(const eckit::PathName& directory, const fdb5::Config& config);

    ~TocCatalogue() override;

    static const char* catalogueTypeName() { return TocEngine::typeName(); }
    const eckit::PathName& basePath() const override;
    eckit::URI uri() const override;

public: // constants
    static const std::string DUMP_PARAM_WALKSUBTOC;

protected: // methods

    std::string type() const override;

    void checkUID() const override;
    bool open() override;
    void close() override;
    void flush() override;
    bool exists() const override;
    void visitEntries(EntryVisitor& visitor, const Store& store, bool sorted) override;
//    void visit(DBVisitor& visitor) override;
    void dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const override;
//    std::string owner() const override;
    std::vector<eckit::PathName> metadataPaths() const override;
    const Schema& schema() const override;

//    eckit::DataHandle *retrieve(const Key &key) const override;
//    void archive(const Key &key, const void *data, eckit::Length length) override;
    void axis(const std::string &keyword, eckit::StringSet &s) const override;

    StatsReportVisitor* statsReportVisitor() const override;
    PurgeVisitor* purgeVisitor() const override;
    WipeVisitor* wipeVisitor(const metkit::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const override;
    void maskIndexEntry(const Index& index) const override;

    void loadSchema() override;

//    DbStats statistics() const override;

    std::vector<Index> indexes(bool sorted=false) const override;

    void allMasked(std::set<std::pair<eckit::PathName, eckit::Offset>>& metadata,
                   std::set<eckit::PathName>& data) const override;

    // Control access properties of the DB

    void control(const ControlAction& action, const ControlIdentifiers& identifiers) const override;

    bool retrieveLocked() const override;
    bool archiveLocked() const override;
    bool listLocked() const override;
    bool wipeLocked() const override;

    Key currentIndexKey_;

private: // members    

    friend class TocWipeVisitor;

    Schema schema_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

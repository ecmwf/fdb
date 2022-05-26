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
#include "fdb5/toc/FileSpace.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/toc/TocEngine.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class TocCatalogue : public Catalogue, public TocHandler {

public: // methods

    TocCatalogue(const Key& key, const fdb5::Config& config);
    TocCatalogue(const eckit::PathName& directory, const Permission& permission, const fdb5::Config& config);

    ~TocCatalogue() override {}

    static const char* catalogueTypeName() { return TocEngine::typeName(); }
    const eckit::PathName& basePath() const override;
    eckit::URI uri() const override;
    const Key& indexKey() const override { return currentIndexKey_; }

    static void remove(const eckit::PathName& path, std::ostream& logAlways, std::ostream& logVerbose, bool doit);

    bool listLocked() const override;
    bool retrieveLocked() const override;
    bool archiveLocked() const override;
    bool wipeLocked() const override;

public: // constants
    static const std::string DUMP_PARAM_WALKSUBTOC;

protected: // methods

    TocCatalogue(const Key& key, const TocPath& tocPath, const fdb5::Config& config);

    std::string type() const override;

    void checkUID() const override;
    bool exists() const override;
    void visitEntries(EntryVisitor& visitor, const Store& store, bool sorted) override;
    void dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const override;
    std::vector<eckit::PathName> metadataPaths() const override;
    const Schema& schema() const override;

    StatsReportVisitor* statsReportVisitor() const override;
    PurgeVisitor* purgeVisitor(const Store& store) const override;
    WipeVisitor* wipeVisitor(const Store& store, const metkit::mars::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const override;
    void maskIndexEntry(const Index& index) const override;

    void loadSchema() override;

    std::vector<Index> indexes(bool sorted=false) const override;

    void allMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata,
                   std::set<eckit::URI>& data) const override;

    // Control access properties of the DB
    void control(const ControlAction& action, const ControlIdentifiers& identifiers) const override;

    Key currentIndexKey_;

private: // members

    friend class TocWipeVisitor;

    Schema schema_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Nicolau Manubens
/// @date Feb 2022

#pragma once

#include "fdb5/database/DB.h"
// #include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"
// #include "fdb5/toc/TocEngine.h"
#include "fdb5/daos/DaosOID.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on DAOS

//class DaosCatalogue : public Catalogue, public DaosHandler {
class DaosCatalogue : public Catalogue {

public: // methods

    DaosCatalogue(const Key& key, const fdb5::Config& config);
    DaosCatalogue(const eckit::URI& uri, const ControlIdentifiers& controlIdentifiers, const fdb5::Config& config);

    // ~DaosCatalogue() override {}

    // static const char* catalogueTypeName() { return DaosEngine::typeName(); }
    
//     const eckit::PathName& basePath() const override;
    eckit::URI uri() const override { NOTIMP; };
//     const Key& indexKey() const override { return currentIndexKey_; }

//     static void remove(const eckit::PathName& path, std::ostream& logAlways, std::ostream& logVerbose, bool doit);

//     bool enabled(const ControlIdentifier& controlIdentifier) const override;

// public: // constants
//     static const std::string DUMP_PARAM_WALKSUBTOC;

// protected: // methods

//     //TocCatalogue(const Key& key, const TocPath& tocPath, const fdb5::Config& config);

    std::string type() const override { NOTIMP; };

    void checkUID() const override { NOTIMP; };
    bool exists() const override { NOTIMP; };
    void visitEntries(EntryVisitor& visitor, const Store& store, bool sorted) override { NOTIMP; };
    void dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const override { NOTIMP; };
    std::vector<eckit::PathName> metadataPaths() const override { NOTIMP; };
    const Schema& schema() const override { NOTIMP; };

    StatsReportVisitor* statsReportVisitor() const override { NOTIMP; };
    PurgeVisitor* purgeVisitor(const Store& store) const override { NOTIMP; };
    WipeVisitor* wipeVisitor(const Store& store, const metkit::mars::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const override { NOTIMP; };
    MoveVisitor* moveVisitor(const Store& store, const metkit::mars::MarsRequest& request, const eckit::URI& dest, bool removeSrc, int removeDelay, int threads) const override { NOTIMP; };
    void maskIndexEntry(const Index& index) const override { NOTIMP; };

    void loadSchema() override;

    std::vector<Index> indexes(bool sorted=false) const override { NOTIMP; };

    void allMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata,
                   std::set<eckit::URI>& data) const override { NOTIMP; };

    // Control access properties of the DB
    void control(const ControlAction& action, const ControlIdentifiers& identifiers) const override { NOTIMP; };

protected: // members

//     Key currentIndexKey_;

    std::string pool_;
    std::string db_cont_;

    fdb5::DaosOID main_kv_{0, 0, DAOS_OT_KV_HASHED, OC_S1};  // TODO: take oclass from config

private: // members

//     // friend class TocWipeVisitor;
//     // friend class TocMoveVisitor;

    Schema schema_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

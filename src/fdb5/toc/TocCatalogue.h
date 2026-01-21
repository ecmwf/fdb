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

#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/toc/FileSpace.h"
#include "fdb5/toc/TocEngine.h"
#include "fdb5/toc/TocHandler.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems
class CatalogueWipeState;
class TocCatalogue : public CatalogueImpl, public TocHandler {

public:  // methods

    TocCatalogue(const Key& dbKey, const fdb5::Config& config);
    TocCatalogue(const eckit::PathName& directory, const ControlIdentifiers& controlIdentifiers,
                 const fdb5::Config& config);

    ~TocCatalogue() override {}

    eckit::URI uri() const override;
    const Key& indexKey() const override { return currentIndexKey_; }

    static void remove(const eckit::PathName& path, std::ostream& logAlways, std::ostream& logVerbose, bool doit);

    bool enabled(const ControlIdentifier& controlIdentifier) const override;

public:  // constants

    static const std::string DUMP_PARAM_WALKSUBTOC;

protected:  // methods

    TocCatalogue(const Key& dbKey, const TocPath& tocPath, const fdb5::Config& config);

    std::string type() const override;

    void checkUID() const override;
    bool exists() const override;
    void dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const override;
    // std::vector<eckit::PathName> metadataPaths() const override;
    const Schema& schema() const override;
    const Rule& rule() const override;

    StatsReportVisitor* statsReportVisitor() const override;
    PurgeVisitor* purgeVisitor(const Store& store) const override;
    MoveVisitor* moveVisitor(const Store& store, const metkit::mars::MarsRequest& request, const eckit::URI& dest,
                             eckit::Queue<MoveElement>& queue) const override;
    void maskIndexEntries(const std::set<Index>& indexes) const override;

    void loadSchema() override;

    bool uriBelongs(const eckit::URI& uri) const override;

    std::vector<Index> indexes(bool sorted = false) const override;

    void allMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata, std::set<eckit::URI>& data) const override;

    // Control access properties of the DB
    void control(const ControlAction& action, const ControlIdentifiers& identifiers) const override;

    // wipe
    CatalogueWipeState wipeInit() const override;
    bool wipeIndex(const Index& index, bool include, CatalogueWipeState& wipeState) const override;
    void wipeFinalise(CatalogueWipeState& wipeState) const override;
    bool doWipeUnknown(const std::set<eckit::URI>& unknownURIs) const override;
    bool doWipe(const CatalogueWipeState& wipeState) const override;
    bool doUnsafeFullWipe() const override { return false; }

    void doWipeEmptyDatabases() const override;

private:  // methods

    void addMaskedPaths(CatalogueWipeState& tocWipeState) const;

protected:  // members

    Key currentIndexKey_;

private:  // members

    friend class TocMoveVisitor;

    // non-owning
    const Schema* schema_;
    const RuleDatabase* rule_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

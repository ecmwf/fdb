/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

/// @file   FamCatalogue.h
/// @author Metin Cakircali
/// @date   Mar 2026

#pragma once

#include <optional>
#include <string>
#include <vector>

#include "fdb5/database/Catalogue.h"
#include "fdb5/fam/FamCommon.h"
#include "fdb5/rules/Schema.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// Base class for the FAM-backed FDB catalogue.
///
/// Inherits CatalogueImpl (generic FDB catalogue machinery) and FamCommon (FAM
/// connectivity and naming helpers). Concrete write and read operations are
/// provided by FamCatalogueWriter and FamCatalogueReader respectively.
class FamCatalogue : public CatalogueImpl, public FamCommon {

public:  // static methods

    /// Derive the catalogue FamMap name for a given DB key string.
    static std::string catalogueName(const Key& key);

    /// Derive the data FamMap name for a given index key string.
    static std::string indexName(const Key& key);

public:  // methods

    FamCatalogue(const Key& key, const fdb5::Config& config);
    FamCatalogue(const eckit::URI& uri, const ControlIdentifiers& control_identifiers, const fdb5::Config& config);

    eckit::URI uri() const override;
    const Key& indexKey() const override { return currentIndexKey_; }

protected:  // methods

    bool selectIndex(const Key& key) override;

    void deselectIndex() override;

    std::string type() const override;
    bool exists() const override;

    void checkUID() const override;
    const Schema& schema() const override;
    const Rule& rule() const override;
    void loadSchema() override;

    void dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const override;

    StatsReportVisitor* statsReportVisitor() const override;
    PurgeVisitor* purgeVisitor(const Store& store) const override;
    MoveVisitor* moveVisitor(const Store& store, const metkit::mars::MarsRequest& request, const eckit::URI& dest,
                             eckit::Queue<MoveElement>& queue) const override;

    void maskIndexEntries(const std::set<Index>& indexes) const override;
    void allMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata, std::set<eckit::URI>& data) const override;

    bool uriBelongs(const eckit::URI& uri) const override;

    std::vector<Index> indexes(bool sorted = false) const override;

    bool enabled(const ControlIdentifier& /*control_identifier*/) const override { return true; }
    void control(const ControlAction& action, const ControlIdentifiers& identifiers) const override;

    CatalogueWipeState wipeInit() const override;
    bool markIndexForWipe(const Index& index, bool include, CatalogueWipeState& wipe_state) const override;
    void finaliseWipeState(CatalogueWipeState& wipe_state) const override;
    bool doWipeUnknowns(const std::set<eckit::URI>& unknown_uris) const override;
    bool doWipeURIs(const CatalogueWipeState& wipe_state) const override;
    void doWipeEmptyDatabase() const override;
    bool doUnsafeFullWipe() const override;

protected:  // methods

    virtual void dumpSchema(std::ostream& stream) const = 0;

    const std::string& name() const { return name_; }

    /// Return the cached catalogue.
    /// FamMap construction costs 2 FAM RPCs (table + count objects).
    Map& catalogue() const {
        if (!catalogue_) {
            catalogue_.emplace(name_, getRegion());
        }
        return *catalogue_;
    }

private:  // members

    /// The name_ is used as the catalogue-level index registry
    std::string name_;

    Key currentIndexKey_;

    Schema schema_;
    const RuleDatabase* rule_{nullptr};

    mutable std::optional<Map> catalogue_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

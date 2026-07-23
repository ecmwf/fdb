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

#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/helpers/MoveIterator.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/fam/FamCommon.h"
#include "fdb5/rules/Schema.h"

#include "eckit/config/Configuration.h"
#include "eckit/container/Queue.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Offset.h"

#include <iosfwd>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace fdb5 {

class Key;
class Field;
class ControlIdentifiers;
class Config;
class Rule;
class RuleDatabase;
class StatsReportVisitor;
class PurgeVisitor;
class MoveVisitor;
class Store;
class Index;


//----------------------------------------------------------------------------------------------------------------------

/// Inherits CatalogueImpl (generic FDB catalogue machinery) and FamCommon (FAM
/// connectivity and naming helpers). Concrete write and read operations are
/// provided by FamCatalogueWriter and FamCatalogueReader respectively.
class FamCatalogue : public CatalogueImpl, public FamCommon {

public:  // static methods

    /// Derive the catalogue name for a given DB key
    static std::string catalogueName(const Key& key);

    static std::string indexName(const std::string& cat_name, Key key);

public:  // methods

    FamCatalogue(const Key& key, const Config& config);

    FamCatalogue(const eckit::URI& uri, const ControlIdentifiers& control_identifiers, const Config& config);

    eckit::URI uri() const override;

    const Key& indexKey() const override { return currentIndexKey_; }

protected:  // methods

    std::string indexName(const Key& key) const;

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

    virtual void dumpSchema(std::ostream& stream) const = 0;

    const std::string& name() const { return name_; }

    /// Return the cached catalogue.
    Map& catalogue() const {
        std::call_once(catOnce_, [this] { catalogue_.emplace(name_, getRegion()); });
        return *catalogue_;
    }

protected:  // members

    /// Currently selected index key (mirrors the TocCatalogue/DaosCatalogue idiom; the
    /// concrete Reader/Writer overrides of selectIndex read and update it directly).
    Key currentIndexKey_;

private:  // members

    std::string name_;

    Schema schema_;

    const RuleDatabase* rule_{nullptr};

    mutable std::once_flag catOnce_;
    mutable std::optional<Map> catalogue_;

    mutable bool cleanupEmptyDatabase_{false};
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

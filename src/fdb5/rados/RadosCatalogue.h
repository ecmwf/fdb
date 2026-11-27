// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Nicolau Manubens
/// @author Metin Cakircali
/// @date Jun 2024

#pragma once

#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/helpers/MoveIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/MoveVisitor.h"
#include "fdb5/database/PurgeVisitor.h"
#include "fdb5/database/StatsReportVisitor.h"
#include "fdb5/rados/RadosCommon.h"
#include "fdb5/rules/Schema.h"

#include "eckit/config/Configuration.h"
#include "eckit/container/Queue.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Offset.h"
#include "eckit/io/rados/RadosKeyValue.h"

#include <map>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace fdb5 {

class Rule;
class RuleDatabase;
class CatalogueWipeState;

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on Rados

class RadosCatalogue : public CatalogueImpl, public RadosCommon {

public:  // methods

    RadosCatalogue(const Key& key, const fdb5::Config& config);
    RadosCatalogue(const eckit::URI& uri, const fdb5::Config& config);

    static const char* catalogueTypeName() { return "rados"; }

    eckit::URI uri() const override;
    const Key& indexKey() const override { return currentIndexKey_; }

    std::string type() const override;

    void checkUID() const override { /* nothing to do */ }
    bool exists() const override;
    void dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const override {
        out << "RadosCatalogue(" << type() << ":" << dbKey_ << ")" << std::endl;
    }
    const Schema& schema() const override;

    StatsReportVisitor* statsReportVisitor() const override { NOTIMP; };
    PurgeVisitor* purgeVisitor(const Store& store) const override { NOTIMP; };
    MoveVisitor* moveVisitor(const Store& store, const metkit::mars::MarsRequest& request, const eckit::URI& dest,
                             eckit::Queue<MoveElement>& queue) const override {
        NOTIMP;
    };

    void loadSchema() override;

    std::vector<Index> indexes(bool sorted = false) const override;

    // No masking metadata is persisted for this backend; wipe removes entries directly, so there is
    // nothing to enumerate here.
    void allMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata,
                   std::set<eckit::URI>& data) const override {}

    // Control access properties of the DB. Persisted as per-identifier reserved KV entries (`control.list`, etc.)
    // in the catalogue KV; absence of the entry means the identifier is enabled.
    void control(const ControlAction& action, const ControlIdentifiers& identifiers) const override;
    bool enabled(const ControlIdentifier& controlIdentifier) const override;

    const Rule& rule() const override;

    bool uriBelongs(const eckit::URI& uri) const override;

    void maskIndexEntries(const std::set<Index>& indexes) const override;

    // Wipe-related methods
    CatalogueWipeState wipeInit() const override;
    bool markIndexForWipe(const Index& index, bool include, CatalogueWipeState& wipeState) const override;
    void finaliseWipeState(CatalogueWipeState& wipeState) const override;
    bool doWipeUnknowns(const std::set<eckit::URI>& unknownURIs) const override;
    bool doWipeURIs(const CatalogueWipeState& wipeState) const override;
    void doWipeEmptyDatabase() const override;
    bool doUnsafeFullWipe() const override;

protected:  // methods

    // The KV of the index registered under `key` in the catalogue KV, if there is one.
    std::optional<eckit::RadosKeyValue> registeredIndex(const Key& key) const;

    // Loads the schema from its textual form rather than from the catalogue KV.
    void loadSchema(const std::string& text);

private:  // methods

    // Removes every object of the database namespace and its entry in the root KV.
    void destroyDatabase() const;

protected:  // types

    using IndexStore = std::map<Key, Index>;

protected:  // members

    Key currentIndexKey_;

    // Indexes selected so far and the one currently selected, maintained by the reader and the writer.
    IndexStore indexes_;
    Index current_;

private:  // members

    Schema schema_;
    const RuleDatabase* rule_{nullptr};
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

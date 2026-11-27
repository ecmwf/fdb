// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/rados/RadosCatalogue.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/DatabaseNotFoundException.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/rados/RadosCommon.h"
#include "fdb5/rados/RadosIndex.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/rules/Schema.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/rados/RadosException.h"
#include "eckit/io/rados/RadosKeyValue.h"
#include "eckit/io/rados/RadosNamespace.h"
#include "eckit/io/rados/RadosObject.h"
#include "eckit/log/Log.h"
#include "eckit/log/Timer.h"

#include <iostream>
#include <optional>
#include <ostream>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RadosCatalogue::RadosCatalogue(const Key& key, const fdb5::Config& config) :
    CatalogueImpl(key, ControlIdentifiers{}, config), RadosCommon(config, key) {}

RadosCatalogue::RadosCatalogue(const eckit::URI& uri, const fdb5::Config& config) :
    CatalogueImpl(Key(), ControlIdentifiers{}, config), RadosCommon(config, uri) {
    try {
        dbKey_ = read_key(db_kv_);
    }
    catch (eckit::NotFound& e) {
        throw fdb5::DatabaseNotFoundException(std::string("RadosCatalogue database not found ") + "(pool: '" + pool_ +
                                              "', namespace: '" + db_namespace_ + "')");
    }
}

bool RadosCatalogue::exists() const {
    return db_kv_.exists();
}

eckit::URI RadosCatalogue::uri() const {
    return db_kv_.nspace().uri();
}

const Schema& RadosCatalogue::schema() const {
    return schema_;
}

const Rule& RadosCatalogue::rule() const {
    ASSERT(rule_);
    return *rule_;
}

void RadosCatalogue::loadSchema() {

    eckit::Timer timer("RadosCatalogue::loadSchema()", eckit::Log::debug<fdb5::LibFdb5>());

    loadSchema(read_text(db_kv_, "schema"));
}

void RadosCatalogue::loadSchema(const std::string& text) {

    std::istringstream stream{text};
    schema_.load(stream);

    rule_ = &schema_.matchingRule(dbKey_);
}

std::optional<eckit::RadosKeyValue> RadosCatalogue::registeredIndex(const Key& key) const {
    try {
        return eckit::RadosKeyValue{eckit::URI{read_text(db_kv_, rados_names::indexPrefix + key.valuesToString())}};
    }
    catch (eckit::NotFound& e) {
        return std::nullopt;
    }
}

std::vector<Index> RadosCatalogue::indexes(bool) const {

    // `sorted` is intentionally ignored; the RADOS backend does not need ordered enumeration.
    std::vector<fdb5::Index> res;

    for (const auto& key : db_kv_.keys()) {

        if (!rados_names::has_prefix(key, rados_names::indexPrefix)) {
            continue;
        }

        const eckit::RadosKeyValue index_kv{eckit::URI{read_text(db_kv_, key)}};

        try {
            // Axes are loaded so that visitors relying on Index::axes() (e.g. fdb.axes()) see the datum axes.
            res.push_back(Index(new fdb5::RadosIndex(read_key(index_kv), index_kv, true)));
        }
        catch (eckit::NotFound& e) {
            // The index KV was removed after the catalogue KV was listed.
            continue;
        }
    }

    return res;
}

std::string RadosCatalogue::type() const {

    return RadosCatalogue::catalogueTypeName();
}

bool RadosCatalogue::uriBelongs(const eckit::URI& uri) const {

    return ownsURI(uri);
}

//----------------------------------------------------------------------------------------------------------------------

CatalogueWipeState RadosCatalogue::wipeInit() const {
    return {dbKey_, config()};
}

void RadosCatalogue::maskIndexEntries(const std::set<Index>& indexes) const {

    // A key-value only names what is stored in the cluster, so a copy modifies the same entries.
    eckit::RadosKeyValue db_kv = db_kv_;

    for (const auto& index : indexes) {
        const std::string key = rados_names::indexPrefix + index.key().valuesToString();
        if (db_kv.has(key)) {
            db_kv.remove(key);
        }
    }
}

bool RadosCatalogue::markIndexForWipe(const Index& index, bool include, CatalogueWipeState& wipeState) const {

    eckit::RadosKeyValue index_kv{index.location().uri()};

    // A cross fdb-mount must never delete another DB's index/axis KVs.
    if (!ownsURI(index_kv.uri())) {
        include = false;
    }

    std::vector<eckit::URI> axis_uris;
    try {
        for (const auto& axis : RadosIndex::axisNames(index_kv)) {
            axis_uris.push_back(RadosIndex::axisKeyValue(index_kv, index.key(), axis).uri());
        }
    }
    catch (const eckit::NotFound& e) {
        LOG_DEBUG_LIB(LibFdb5) << "RadosCatalogue::markIndexForWipe: axes lookup missing for index " << index.key()
                               << " (assuming stale index kv): " << e.what() << std::endl;
    }

    const eckit::URI index_uri = index.location().uri();

    if (include) {
        wipeState.markForMasking(index);
        wipeState.markForDeletion(WipeElementType::CATALOGUE_INDEX, index_uri);
        for (const auto& uri : axis_uris) {
            wipeState.markForDeletion(WipeElementType::CATALOGUE_INDEX, uri);
        }
    }
    else {
        wipeState.markAsSafe({index_uri});
        for (const auto& uri : axis_uris) {
            wipeState.markAsSafe({uri});
        }
    }

    return include;
}

void RadosCatalogue::finaliseWipeState(CatalogueWipeState& wipeState) const {

    const eckit::URI db_kv_uri = db_kv_.uri();

    const bool wipeAll = wipeState.safeURIs().empty();
    if (wipeAll) {
        wipeState.markForDeletion(WipeElementType::CATALOGUE, db_kv_uri);
    }
    else {
        wipeState.markAsSafe({db_kv_uri});
        return;
    }

    for (const auto& uri : objectURIs()) {
        if (!wipeState.isMarkedForDeletion(uri)) {
            wipeState.insertUnrecognised(uri);
        }
    }
}

namespace {

void destroy_object(const eckit::URI& uri) {
    eckit::RadosObject obj{uri};
    std::cout << "destroy Rados object: " << obj.str() << std::endl;
    obj.ensureAllDestroyed();
}

}  // namespace

bool RadosCatalogue::doWipeUnknowns(const std::set<eckit::URI>& unknownURIs) const {

    for (const auto& uri : unknownURIs) {
        if (eckit::RadosObject{uri}.exists()) {
            destroy_object(uri);
        }
    }
    return true;
}

bool RadosCatalogue::doWipeURIs(const CatalogueWipeState& wipeState) const {

    const bool wipeAll = wipeState.safeURIs().empty();

    for (const auto& [type, uris] : wipeState.deleteMap()) {
        for (const auto& uri : uris) {
            destroy_object(uri);
        }
    }

    if (wipeAll) {
        cleanupEmptyDatabase_ = true;
    }

    return true;
}

void RadosCatalogue::doWipeEmptyDatabase() const {

    if (!cleanupEmptyDatabase_) {
        return;
    }

    destroyDatabase();

    cleanupEmptyDatabase_ = false;
}

bool RadosCatalogue::doUnsafeFullWipe() const {
    destroyDatabase();
    return true;
}

void RadosCatalogue::destroyDatabase() const {

    const eckit::RadosNamespace db = dbNamespace();
    if (db.pool().exists()) {
        db.destroy();
    }

    // A key-value only names what is stored in the cluster, so a copy modifies the same entries.
    eckit::RadosKeyValue root_kv = root_kv_;
    if (root_kv.has(db_namespace_)) {
        root_kv.remove(db_namespace_);
    }
}

//----------------------------------------------------------------------------------------------------------------------

namespace {

// Reserved KV entry name for a given control identifier; must be filtered out of index enumeration.
std::string control_kv_key(ControlIdentifier id) {
    switch (id) {
        case ControlIdentifier::List:
            return "control.list";
        case ControlIdentifier::Retrieve:
            return "control.retrieve";
        case ControlIdentifier::Archive:
            return "control.archive";
        case ControlIdentifier::Wipe:
            return "control.wipe";
        case ControlIdentifier::UniqueRoot:
            return "control.unique_root";
        default:
            return "";
    }
}

}  // namespace

void RadosCatalogue::control(const ControlAction& action, const ControlIdentifiers& identifiers) const {

    // A key-value only names what is stored in the cluster, so a copy modifies the same entries.
    eckit::RadosKeyValue db_kv = db_kv_;

    for (ControlIdentifier id : identifiers) {
        const std::string key = control_kv_key(id);
        if (key.empty()) {
            continue;
        }
        switch (action) {
            case ControlAction::Disable: {
                const char flag = '1';
                db_kv.put(key, &flag, 1);
                break;
            }
            case ControlAction::Enable:
                if (db_kv.has(key)) {
                    db_kv.remove(key);
                }
                break;
            default:
                eckit::Log::warning() << "RadosCatalogue::control: unexpected action " << static_cast<uint16_t>(action)
                                      << std::endl;
        }
    }
}

bool RadosCatalogue::enabled(const ControlIdentifier& controlIdentifier) const {
    const std::string key = control_kv_key(controlIdentifier);
    if (key.empty()) {
        return true;
    }
    return !db_kv_.has(key);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

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
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/utils/Tokenizer.h"

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
    CatalogueImpl(key, ControlIdentifiers{}, config), RadosCommon(config, "catalogue", key) {}

RadosCatalogue::RadosCatalogue(const eckit::URI& uri, const ControlIdentifiers& controlIdentifiers,
                               const fdb5::Config& config) :
    CatalogueImpl(Key(), controlIdentifiers, config), RadosCommon(config, "catalogue", uri) {
    try {
        dbKey_ = read_db_key(*db_kv_);
    }
    catch (eckit::RadosEntityNotFoundException& e) {
        throw fdb5::DatabaseNotFoundException(std::string("RadosCatalogue database not found ") + "(pool: '" + pool_ +
                                              "', namespace: '" + db_namespace_ + "')");
    }
}

bool RadosCatalogue::exists() const {
    return db_kv_->exists();
}

eckit::URI RadosCatalogue::uri() const {
    return db_kv_->nspace().uri();
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

    std::vector<char> data;
    db_kv_->getMemoryStream(data, "schema", "DB Key-Value");

    std::istringstream stream{std::string(data.begin(), data.end())};
    schema_.load(stream);

    rule_ = &schema_.matchingRule(dbKey_);
}

std::vector<Index> RadosCatalogue::indexes(bool) const {

    // `sorted` is intentionally ignored; the RADOS backend does not need ordered enumeration.
    std::vector<fdb5::Index> res;

    for (const auto& key : db_kv_->keys()) {

        if (key == "schema" || key == "key") {
            continue;
        }

        std::vector<char> v;
        auto m = db_kv_->getMemoryStream(v, key, "DB kv");

        eckit::URI uri(std::string(v.begin(), v.end()));

        eckit::RadosKeyValue index_kv{uri};
        std::optional<fdb5::Key> index_key;
        try {
            std::vector<char> data;
            eckit::MemoryStream ms = index_kv.getMemoryStream(data, "key", "index KV");
            index_key.emplace(ms);
        }
        catch (eckit::RadosEntityNotFoundException& e) {
            continue;
        }

        res.push_back(Index(new fdb5::RadosIndex(index_key.value(), index_kv, false)));
    }

    return res;
}

std::string RadosCatalogue::type() const {

    return RadosCatalogue::catalogueTypeName();
}

bool RadosCatalogue::uriBelongs(const eckit::URI& uri) const {

    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    const auto n = parts.size();

    return (uri.scheme() == type()) && (n >= 2) && (parts[0] == pool_) && (parts[1] == db_namespace_);
}

//----------------------------------------------------------------------------------------------------------------------

CatalogueWipeState RadosCatalogue::wipeInit() const {
    return {dbKey_, config()};
}

void RadosCatalogue::maskIndexEntries(const std::set<Index>& indexes) const {

    for (const auto& index : indexes) {
        std::string key = index.key().valuesToString();
        if (db_kv_->has(key)) {
            db_kv_->remove(key);
        }
    }
}

bool RadosCatalogue::markIndexForWipe(const Index& index, bool include, CatalogueWipeState& wipeState) const {

    eckit::RadosKeyValue index_kv{index.location().uri()};

    // A cross fdb-mount must never delete another DB's index/axis KVs.
    if (index_kv.nspace().pool().name() != pool_ || index_kv.nspace().name() != db_namespace_) {
        include = false;
    }

    std::vector<eckit::URI> axis_uris;
    try {
        std::vector<char> axes_data;
        index_kv.getMemoryStream(axes_data, "axes", "index kv");
        std::vector<std::string> axis_names;
        eckit::Tokenizer parse(",");
        parse(std::string(axes_data.begin(), axes_data.end()), axis_names);
        const std::string idx_key = index.key().valuesToString();
        for (const auto& axis : axis_names) {
            axis_uris.push_back(
                eckit::RadosKeyValue{index_kv.nspace().pool().name(), index_kv.nspace().name(), idx_key + "." + axis}
                    .uri());
        }
    }
    catch (const eckit::RadosEntityNotFoundException& e) {
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

    const eckit::URI db_kv_uri = db_kv_->uri();

    const bool wipeAll = wipeState.safeURIs().empty();
    if (wipeAll) {
        wipeState.markForDeletion(WipeElementType::CATALOGUE, db_kv_uri);
    }
    else {
        wipeState.markAsSafe({db_kv_uri});
        return;
    }

    eckit::RadosNamespace db{pool_, db_namespace_};
    if (!db.exists()) {
        return;
    }

    for (const auto& obj : db.listObjects()) {
        if (obj.name().find(";part-") != std::string::npos) {
            continue;
        }
        const eckit::URI uri = obj.uri();
        if (!wipeState.isMarkedForDeletion(uri)) {
            wipeState.insertUnrecognised(uri);
        }
    }
}

namespace {

void remove_catalogue_uri(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) {

    eckit::RadosObject obj{uri};
    logVerbose << "destroy Rados object: ";
    logAlways << obj.str() << std::endl;
    if (doit) {
        obj.ensureAllDestroyed();
    }
}

}  // namespace

bool RadosCatalogue::doWipeUnknowns(const std::set<eckit::URI>& unknownURIs) const {

    for (const auto& uri : unknownURIs) {
        if (eckit::RadosObject{uri}.exists()) {
            remove_catalogue_uri(uri, std::cout, std::cout, true);
        }
    }
    return true;
}

bool RadosCatalogue::doWipeURIs(const CatalogueWipeState& wipeState) const {

    const bool wipeAll = wipeState.safeURIs().empty();

    for (const auto& [type, uris] : wipeState.deleteMap()) {
        for (const auto& uri : uris) {
            remove_catalogue_uri(uri, std::cout, std::cout, true);
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

    eckit::RadosNamespace db{pool_, db_namespace_};
    if (db.exists()) {
        db.destroy();
    }

    if (root_kv_ && root_kv_->exists() && root_kv_->has(db_namespace_)) {
        root_kv_->remove(db_namespace_);
    }

    cleanupEmptyDatabase_ = false;
}

bool RadosCatalogue::doUnsafeFullWipe() const {

    eckit::RadosNamespace db{pool_, db_namespace_};
    if (db.exists()) {
        db.destroy();
    }

    if (root_kv_ && root_kv_->exists() && root_kv_->has(db_namespace_)) {
        root_kv_->remove(db_namespace_);
    }

    return true;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

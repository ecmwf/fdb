/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/rados/RadosStore.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Store.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/rados/RadosCleanup.h"
#include "fdb5/rados/RadosCommon.h"
#include "fdb5/rados/RadosFieldLocation.h"
#include "fdb5/rules/Rule.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/io/rados/RadosNamespace.h"
#include "eckit/io/rados/RadosObject.h"
#include "eckit/io/rados/RadosPool.h"
#include "eckit/log/Log.h"
#include "eckit/log/TimeStamp.h"
#include "eckit/runtime/Main.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/StaticMutex.h"
#include "eckit/utils/MD5.h"
#include "eckit/utils/Tokenizer.h"

#include <unistd.h>

#include <cstddef>
#include <exception>
#include <iostream>
#include <memory>
#include <ostream>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static StoreBuilder<RadosStore> builder("rados");

RadosStore::RadosStore(const Key& key, const Config& config) : RadosCommon(config, "store", key) {}

RadosStore::RadosStore(const Schema& /*schema*/, const Key& key, const Config& config) : RadosStore(key, config) {}

RadosStore::RadosStore(const eckit::URI& uri, const Config& config) : RadosCommon(config, "store", uri) {}

eckit::URI RadosStore::uri() const {
    return eckit::RadosNamespace(pool_, db_namespace_).uri();
}

eckit::URI RadosStore::uri(const eckit::URI& dataURI) {
    eckit::RadosObject o{dataURI};
    return o.nspace().uri();
}

bool RadosStore::uriBelongs(const eckit::URI& uri) const {

    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    const auto n = parts.size();

    ASSERT(n == 2 || n == 3);
    return ((uri.scheme() == type()) && (parts[0] == pool_) && (parts[1] == db_namespace_));
}

bool RadosStore::uriExists(const eckit::URI& uri) const {

    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    const auto n = parts.size();

    ASSERT(uri.scheme() == type());

    ASSERT(n == 2 || n == 3);
    ASSERT(parts[0] == pool_);
    ASSERT(parts[1] == db_namespace_);

    if (n == 2) {
        return eckit::RadosNamespace(uri).exists();
    }

    return eckit::RadosObject(uri).exists();
}

std::set<eckit::URI> RadosStore::collocatedDataURIs() const {

    std::set<eckit::URI> store_unit_uris;

    eckit::RadosNamespace n{pool_, db_namespace_};

    if (!n.exists()) {
        return store_unit_uris;
    }

    for (const auto& obj : n.listObjects()) {
        if (obj.name().find(";part-") != std::string::npos) {
            continue;
        }
        store_unit_uris.insert(obj.uri());
    }

    return store_unit_uris;
}

std::set<eckit::URI> RadosStore::asCollocatedDataURIs(const std::set<eckit::URI>& uris) const {
    std::set<eckit::URI> res;
    for (const auto& uri : uris) {
        res.insert(uri);
    }
    return res;
}

bool RadosStore::exists() const {
    return eckit::RadosNamespace(pool_, db_namespace_).exists();
}

eckit::DataHandle* RadosStore::retrieve(Field& field) const {
    return field.dataHandle();
}

std::unique_ptr<const FieldLocation> RadosStore::archive(const Key& key, const void* data, eckit::Length length) {

    archivedFields_++;

    const eckit::RadosObject& o = getDataObject(key);

    eckit::DataHandle& h = getDataHandle(key, o);

    eckit::Offset offset{h.position()};

    long len = h.write(data, length);

    ASSERT(len == length);

    return std::make_unique<RadosFieldLocation>(o.uri(), offset, length, fdb5::Key{});
}

RadosStore::~RadosStore() {
    std::exception_ptr ignored;
    best_effort(ignored, "~RadosStore::closeDataHandles", [&] { closeDataHandles(); });
}

size_t RadosStore::flush() {

    if (archivedFields_ == 0) {
        return 0;
    }

    flushDataHandles();

    size_t out = archivedFields_;
    archivedFields_ = 0;
    return out;
}

void RadosStore::close() {
    closeDataHandles();
}

void RadosStore::remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const {

    ASSERT(uri.scheme() == type());

    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    const auto n = parts.size();

    ASSERT(n == 2 || n == 3);

    ASSERT(parts[0] == pool_);
    ASSERT(parts[1] == db_namespace_);

    if (n == 2) {  // namespace

        eckit::RadosNamespace ns{uri};

        logVerbose << "destroy Rados namespace: ";
        logAlways << ns.str() << std::endl;

        if (doit && ns.exists()) {
            ns.destroy();
        }
    }
    else {  // object

        eckit::RadosObject obj{uri};

        logVerbose << "destroy Rados object: ";
        logAlways << obj.str() << std::endl;

        if (doit) {
            obj.ensureAllDestroyed();
        }
    }
}

void RadosStore::print(std::ostream& out) const {

    out << "RadosStore(" << pool_ << "/" << db_namespace_ << ")";
}

//----------------------------------------------------------------------------------------------------------------------

// The database maps to a RADOS namespace; only the namespace holding this database's objects is
// ever touched by the wipe-related methods below.

void RadosStore::finaliseWipeState(StoreWipeState& storeState, bool doit, bool unsafeWipeAll) {

    // `doit` and `unsafeWipeAll` do not affect the preparation of a RADOS store wipe.

    const std::set<eckit::URI>& dataURIs = storeState.includedDataURIs();  // included according to cat
    const std::set<eckit::URI>& safeURIs = storeState.safeURIs();          // excluded according to cat

    // Objects included by the catalogue may no longer exist (e.g. due to a prior incomplete wipe).
    std::set<eckit::URI> nonExistingURIs;
    for (const auto& uri : dataURIs) {
        if (!eckit::RadosObject{uri}.exists()) {
            nonExistingURIs.insert(uri);
        }
    }
    for (const auto& uri : nonExistingURIs) {
        storeState.markAsMissing(uri);
    }

    const bool all = safeURIs.empty();
    if (!all) {
        return;
    }

    // Full wipe: scan the database namespace for any objects unaccounted for by the catalogue.
    eckit::RadosNamespace db{pool_, db_namespace_};

    if (!db.exists()) {
        return;
    }

    for (const auto& obj : db.listObjects()) {

        // Parts belong to a main object and are removed together with it.
        if (obj.name().find(";part-") != std::string::npos) {
            continue;
        }

        const eckit::URI uri = obj.uri();
        if (dataURIs.find(uri) == dataURIs.end() && safeURIs.find(uri) == safeURIs.end()) {
            storeState.insertUnrecognised(uri);
        }
    }
}

bool RadosStore::doWipeUnknowns(const std::set<eckit::URI>& unknownURIs) const {
    for (const auto& uri : unknownURIs) {
        if (eckit::RadosObject{uri}.exists()) {
            remove(uri, std::cout, std::cout, true);
        }
    }
    return true;
}

bool RadosStore::doWipeURIs(const StoreWipeState& wipeState) const {
    const bool wipeAll = wipeState.safeURIs().empty();

    for (const auto& uri : wipeState.includedDataURIs()) {
        remove(uri, std::cout, std::cout, true);
    }

    if (wipeAll) {
        cleanupEmptyDatabase_ = true;
    }

    return true;
}

void RadosStore::doWipeEmptyDatabase() const {

    if (!cleanupEmptyDatabase_) {
        return;
    }

    eckit::RadosNamespace db{pool_, db_namespace_};

    if (db.exists()) {
        remove(db.uri(), std::cout, std::cout, true);
    }
}

bool RadosStore::doUnsafeFullWipe() const {

    // If the database namespace also holds a catalogue, skip: the catalogue-driven wipe owns the
    // namespace. Presence of a "key" entry in the DB KV is used as the catalogue-exists signal.
    if (db_kv_ && (!db_kv_->exists() || !db_kv_->has("key"))) {

        eckit::RadosNamespace db{pool_, db_namespace_};

        if (db.exists()) {
            remove(db.uri(), std::cout, std::cout, true);
        }
    }

    return true;
}

std::vector<eckit::URI> RadosStore::getAuxiliaryURIs(const eckit::URI& /*uri*/, bool /*onlyExisting*/) const {
    return {};
}


//----------------------------------------------------------------------------------------------------------------------

// Unique name generation copied from eckit::LocalPathName::unique.
static eckit::StaticMutex local_mutex;

eckit::RadosObject RadosStore::generateDataObject(const Key& key) const {

    eckit::AutoLock<eckit::StaticMutex> lock(local_mutex);

    std::string hostname = eckit::Main::hostname();

    static unsigned long long n = (((unsigned long long)::getpid()) << 32);

    static std::string format = "%Y%m%d.%H%M%S";
    std::ostringstream os;
    os << eckit::TimeStamp(format) << '.' << hostname << '.' << n++;

    std::string name = os.str();

    eckit::MD5 md5(name);

    return eckit::RadosObject{pool_, db_namespace_, key.valuesToString() + "." + md5.digest() + ".data"};
}

const eckit::RadosObject& RadosStore::getDataObject(const Key& key) const {

    auto it = dataObjects_.find(key);
    if (it == dataObjects_.end()) {
        it = dataObjects_.emplace(key, generateDataObject(key)).first;
    }
    return it->second;
}

eckit::DataHandle& RadosStore::getDataHandle(const Key& key, const eckit::RadosObject& name) {

    auto iter = handles_.find(key);
    if (iter != handles_.end()) {
        return *(iter->second);
    }

    auto handle = std::unique_ptr<eckit::DataHandle>{name.multipartWriteHandle(maxPartSize_)};
    ASSERT(handle);

    handle->openForWrite(0);
    auto [inserted, success] = handles_.emplace(key, std::move(handle));
    ASSERT(success);

    return *inserted->second;
}

void RadosStore::closeDataHandles() {

    // Detach the map first so partial failures never leave the destructor retrying the same handle.
    HandleStore handles;
    handles.swap(handles_);
    dataObjects_.clear();

    std::exception_ptr first;
    for (auto& [key, handle] : handles) {
        best_effort(first, "RadosStore::closeDataHandles", [&] { handle->close(); });
    }
    if (first) {
        std::rethrow_exception(first);
    }
}

void RadosStore::flushDataHandles() {

    std::exception_ptr first;
    for (auto& [key, handle] : handles_) {
        best_effort(first, "RadosStore::flushDataHandles", [&] { handle->flush(); });
    }
    if (first) {
        std::rethrow_exception(first);
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

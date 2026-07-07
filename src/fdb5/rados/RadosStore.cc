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

#include "eckit/config/Resource.h"
#include "eckit/io/EmptyHandle.h"
#include "eckit/io/rados/RadosNamespace.h"
#include "eckit/io/rados/RadosPool.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/TimeStamp.h"
#include "eckit/log/Timer.h"
#include "eckit/runtime/Main.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/StaticMutex.h"
#include "eckit/utils/MD5.h"
#include "eckit/utils/Tokenizer.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/rados/RadosFieldLocation.h"
#include "fdb5/rules/Rule.h"

#include <unistd.h>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static StoreBuilder<RadosStore> builder("rados");

RadosStore::RadosStore(const Key& key, const Config& config) :
    Store(), RadosCommon(config, "store", key), archivedFields_(0) {

    parseConfig(config);
}

RadosStore::RadosStore(const Schema& schema, const Key& key, const Config& config) : RadosStore(key, config) {}

RadosStore::RadosStore(const eckit::URI& uri, const Config& config) :
    Store(), RadosCommon(config, "store", uri), archivedFields_(0) {

    parseConfig(config);
}

eckit::URI RadosStore::uri() const {

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    return eckit::RadosNamespace(pool_, db_namespace_).uri();

#else

    return eckit::RadosPool(db_pool_).uri();

#endif
}

eckit::URI RadosStore::uri(const eckit::URI& dataURI) {

    eckit::RadosObject o{dataURI};

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    return o.nspace().uri();

#else

    return o.nspace().pool().uri();

#endif
}

bool RadosStore::uriBelongs(const eckit::URI& uri) const {

    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    const auto n = parts.size();

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    ASSERT(n == 2 || n == 3);
    return ((uri.scheme() == type()) && (parts[0] == pool_) && (parts[1] == db_namespace_));

#else

    ASSERT(n == 2 || n == 3);
    return ((uri.scheme() == type()) && (parts[0] == db_pool_) && (parts[1] == namespace_));

#endif
}

bool RadosStore::uriExists(const eckit::URI& uri) const {

    /// @todo: revisit the name of this method

    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    const auto n = parts.size();

    ASSERT(uri.scheme() == type());

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    ASSERT(n == 2 || n == 3);
    ASSERT(parts[0] == pool_);
    ASSERT(parts[1] == db_namespace_);

    if (n == 2) {
        return eckit::RadosNamespace(uri).exists();
    }

#else

    ASSERT(n == 1 || n == 3);
    ASSERT(parts[0] == db_pool_);
    if (n > 1) {
        ASSERT(parts[1] == namespace_);
    }

    if (n == 1) {
        return eckit::RadosPool(uri).exists();
    }

#endif

    return eckit::RadosObject(uri).exists();
}

std::set<eckit::URI> RadosStore::collocatedDataURIs() const {

    std::set<eckit::URI> store_unit_uris;

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    eckit::RadosNamespace n{pool_, db_namespace_};

#else

    eckit::RadosNamespace n{db_pool_, namespace_};

#endif

    if (!n.exists()) {
        return store_unit_uris;
    }

    /// @note if a RadosCatalogue is implemented, some filtering will need to
    ///   be done here to discriminate store objects from catalogue objects
    for (const auto& obj : n.listObjects()) {

#ifdef fdb5_HAVE_RADOS_STORE_MULTIPART
        if (obj.name().find(";part-") != std::string::npos) {
            continue;
        }
#endif

        store_unit_uris.insert(obj.uri());
    }

    return store_unit_uris;
}

std::set<eckit::URI> RadosStore::asCollocatedDataURIs(const std::set<eckit::URI>& uris) const {

    std::set<eckit::URI> res;

    /// @note: this is only uniquefying the input uris (coming from an index)
    ///   in case theres any duplicate.
    for (auto& uri : uris) {
        res.insert(uri);
    }

    return res;
}

bool RadosStore::exists() const {

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    return eckit::RadosNamespace(pool_, db_namespace_).exists();

#else

    return eckit::RadosNamespace(db_pool_, namespace_).exists();

#endif
}

/// @todo: never used in actual fdb-read?
eckit::DataHandle* RadosStore::retrieve(Field& field) const {

    return field.dataHandle();
}

std::unique_ptr<const FieldLocation> RadosStore::archive(const Key& key, const void* data, eckit::Length length) {

    archivedFields_++;

#ifdef fdb5_HAVE_RADOS_STORE_OBJ_PER_FIELD

    /// @note: generate unique object name starting by indexkey_
    eckit::RadosObject o = generateDataObject(key);

#ifndef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    /// @todo: ensure pool if not yet seen by this process
    static std::set<std::string> knownPools;
    const eckit::RadosPool& p = o.nspace().pool();
    if (knownPools.find(p.name()) == knownPools.end()) {
        p.ensureCreated();
        knownPools.insert(p.name());
    }

#endif

#ifdef fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH
    eckit::DataHandle* h = o.asyncDataHandle();
    ASSERT(handles_.size() < maxHandleBuffSize_);
    handles_.push_back(h);
#else
    std::unique_ptr<eckit::DataHandle> h(o.dataHandle());
#endif

    /// @todo: should throw here if object already exists

    h->openForWrite(length);
    eckit::AutoClose closer(*h);

    h->write(data, length);

    return std::unique_ptr<RadosFieldLocation>(new RadosFieldLocation(o.uri(), 0, length, fdb5::Key{}));

#else

    /// @note: get or generate unique key name
    const eckit::RadosObject& o = getDataObject(key);

#ifndef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    /// @todo: ensure pool if not yet seen by this process
    static std::set<std::string> knownPools;
    const eckit::RadosPool& p = o.nspace().pool();
    if (knownPools.find(p.name()) == knownPools.end()) {
        p.ensureCreated();
        knownPools.insert(p.name());
    }

#endif

    eckit::DataHandle& h = getDataHandle(key, o);

    eckit::Offset offset{h.position()};

    long len = h.write(data, length);

    ASSERT(len == length);

    return std::unique_ptr<RadosFieldLocation>(new RadosFieldLocation(o.uri(), offset, length, fdb5::Key{}));

#endif
}

size_t RadosStore::flush() {

    if (archivedFields_ == 0) {
        return 0;
    }

#ifdef fdb5_HAVE_RADOS_STORE_OBJ_PER_FIELD

#ifdef fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH
    for (const auto& h : handles_) {
        h->flush();
    }
#else
    // NOOP
#endif

#else

#ifdef fdb5_HAVE_RADOS_STORE_MULTIPART

    /// @note: needs to be called even if PERSIST_ON_FLUSH=OFF, as the
    ///   multipart handles need to persist the multipart attributes which
    ///   is performed in the multihandle flush.
    flushDataHandles();

#else

#ifdef fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH
    flushDataHandles();
#else
    // NOOP
#endif

#endif

#endif

    size_t out = archivedFields_;
    archivedFields_ = 0;
    return out;
}

void RadosStore::close() {

#ifdef fdb5_HAVE_RADOS_STORE_OBJ_PER_FIELD

#ifdef fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH
    for (const auto& h : handles_) {
        h->close();
    }
#else
    // NOOP
#endif

#else

    closeDataHandles();

#endif
}

void RadosStore::remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const {

    ASSERT(uri.scheme() == type());

    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    const auto n = parts.size();

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    ASSERT(n == 2 || n == 3);

    ASSERT(parts[0] == pool_);
    ASSERT(parts[1] == db_namespace_);

    if (n == 2) {  // namespace

        eckit::RadosNamespace ns{uri};

        logVerbose << "destroy Rados namespace: ";
        logAlways << ns.str() << std::endl;

        if (doit) {
            ns.destroy();  /// @todo: ensureDestroyed?
        }
    }
    else {  // object

        eckit::RadosObject obj{uri};

        logVerbose << "destroy Rados object: ";
        logAlways << obj.str() << std::endl;

#if defined(fdb5_HAVE_RADOS_STORE_MULTIPART) && !defined(fdb5_HAVE_RADOS_STORE_OBJ_PER_FIELD)
        if (doit) {
            obj.ensureAllDestroyed();
        }
#else
        if (doit) {
            obj.ensureDestroyed();
        }
#endif
    }

#else

    ASSERT(n == 1 || n == 3);

    ASSERT(parts[0] == db_pool_);

    if (n == 1) {  // pool

        eckit::RadosPool pool{uri};

        logVerbose << "destroy Rados pool: ";
        logAlways << pool.name() << std::endl;

        if (doit) {
            pool.ensureDestroyed();
        }
    }
    else {  // object

        ASSERT(parts[1] == namespace_);

        eckit::RadosObject obj{uri};

        logVerbose << "destroy Rados object: ";
        logAlways << obj.str() << std::endl;

#if defined(fdb5_HAVE_RADOS_STORE_MULTIPART) && !defined(fdb5_HAVE_RADOS_STORE_OBJ_PER_FIELD)
        if (doit) {
            obj.ensureAllDestroyed();
        }
#else
        if (doit) {
            obj.ensureDestroyed();
        }
#endif
    }

#endif
}

void RadosStore::print(std::ostream& out) const {

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    out << "RadosStore(" << pool_ << "/" << db_namespace_ << ")";

#else

    out << "RadosStore(" << db_pool_ << "/" << namespace_ << ")";

#endif
}

//----------------------------------------------------------------------------------------------------------------------

/// @note: for SINGLE_POOL the database maps to a Rados namespace, otherwise to a Rados pool.
///   Only the namespace/pool holding this database's objects is ever touched here.

void RadosStore::finaliseWipeState(StoreWipeState& storeState, bool doit, bool unsafeWipeAll) {
    /// @note: doit and unsafeWipeAll do not affect the preparation of a Rados store wipe.

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

    // Full wipe: scan the database namespace/pool for any objects unaccounted for by the catalogue.
#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
    eckit::RadosNamespace db{pool_, db_namespace_};
#else
    eckit::RadosNamespace db{db_pool_, namespace_};
#endif

    if (!db.exists()) {
        return;
    }

    for (const auto& obj : db.listObjects()) {

#if defined(fdb5_HAVE_RADOS_STORE_MULTIPART) && !defined(fdb5_HAVE_RADOS_STORE_OBJ_PER_FIELD)
        // Parts belong to a main object and are removed together with it.
        if (obj.name().find(";part-") != std::string::npos) {
            continue;
        }
#endif

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

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
    eckit::RadosNamespace db{pool_, db_namespace_};
#else
    eckit::RadosNamespace db{db_pool_, namespace_};
#endif

    if (db.exists()) {
        remove(db.uri(), std::cout, std::cout, true);
    }
}

bool RadosStore::doUnsafeFullWipe() const {

    /// @note: if the database namespace/pool also holds a catalogue, the wiping is skipped as the
    ///   catalogue is in charge. The presence of a "key" entry in the database key-value is used to
    ///   determine whether a catalogue exists here.
    if (db_kv_ && (!db_kv_->exists() || !db_kv_->has("key"))) {

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
        eckit::RadosNamespace db{pool_, db_namespace_};
#else
        eckit::RadosNamespace db{db_pool_, namespace_};
#endif

        if (db.exists()) {
            remove(db.uri(), std::cout, std::cout, true);
        }
    }

    return true;
}

//----------------------------------------------------------------------------------------------------------------------

/// @note: unique name generation copied from LocalPathName::unique.
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

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

#ifdef fdb5_HAVE_RADOS_STORE_OBJ_PER_FIELD

    return eckit::RadosObject{pool_, db_namespace_, md5.digest()};

#else

    return eckit::RadosObject{pool_, db_namespace_, key.valuesToString() + "." + md5.digest() + ".data"};

#endif

#else

#ifdef fdb5_HAVE_RADOS_STORE_OBJ_PER_FIELD

    return eckit::RadosObject{db_pool_, namespace_, md5.digest()};

#else

    return eckit::RadosObject{db_pool_, namespace_, key.valuesToString() + "." + md5.digest() + ".data"};

#endif

#endif
}

#ifndef fdb5_HAVE_RADOS_STORE_OBJ_PER_FIELD

const eckit::RadosObject& RadosStore::getDataObject(const Key& key) const {

    auto it = dataObjects_.find(key);
    if (it == dataObjects_.end()) {
        it = dataObjects_.emplace(key, generateDataObject(key)).first;
    }
    return it->second;
}

eckit::DataHandle& RadosStore::getDataHandle(const Key& key, const eckit::RadosObject& name) {

    HandleStore::const_iterator j = handles_.find(key);
    if (j != handles_.end()) {
        return *(j->second);
    }

#ifdef fdb5_HAVE_RADOS_STORE_MULTIPART

#ifdef fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH
    eckit::DataHandle* dh = name.asyncMultipartWriteHandle(maxPartSize_, maxAioBuffSize_, maxPartHandleBuffSize_);
#else
    eckit::DataHandle* dh = name.multipartWriteHandle(maxPartSize_);
#endif

#else

#ifdef fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH
    eckit::DataHandle* dh = name.asyncDataHandle(maxAioBuffSize_);
#else
    eckit::DataHandle* dh = name.dataHandle();
#endif

#endif

    ASSERT(dh);

    handles_[key] = dh;

    dh->openForWrite(0);

    return *dh;
}

void RadosStore::closeDataHandles() {

    for (HandleStore::iterator j = handles_.begin(); j != handles_.end(); ++j) {
        eckit::DataHandle* dh = j->second;
        dh->close();
        delete dh;
    }

    handles_.clear();
    dataObjects_.clear();
}

void RadosStore::flushDataHandles() {

    for (HandleStore::iterator j = handles_.begin(); j != handles_.end(); ++j) {
        eckit::DataHandle* dh = j->second;
        dh->flush();
    }
}

#endif

void RadosStore::parseConfig(const fdb5::Config& config) {

    eckit::LocalConfiguration rados{}, store_conf{};

    if (config.has("rados")) {
        rados = config.getSubConfiguration("rados");
        if (rados.has("store")) {
            store_conf = rados.getSubConfiguration("store");
        }
    }

#if defined(fdb5_HAVE_RADOS_STORE_OBJ_PER_FIELD) && defined(fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH)
    maxHandleBuffSize_ = store_conf.getInt("maxHandleBuffSize", 1024 * 1024);
#endif

#if (!defined(fdb5_HAVE_RADOS_STORE_OBJ_PER_FIELD)) && defined(fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH)
#ifdef fdb5_HAVE_RADOS_STORE_MULTIPART
    maxAioBuffSize_ = store_conf.getInt("maxAioBuffSize", 1024);
    maxPartHandleBuffSize_ = store_conf.getInt("maxPartHandleBuffSize", 1024);
#else
    maxAioBuffSize_ = store_conf.getInt("maxAioBuffSize", 1024 * 1024);
#endif
#endif
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

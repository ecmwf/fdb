/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Bytes.h"
#include "eckit/log/Timer.h"

#include "eckit/config/Resource.h"
#include "eckit/io/EmptyHandle.h"
#include "eckit/io/rados/RadosWriteHandle.h"
// #include <unistd.h>

#include "eckit/log/TimeStamp.h"
#include "eckit/runtime/Main.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/StaticMutex.h"
#include "eckit/utils/MD5.h"
#include "eckit/utils/Tokenizer.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/io/FDBFileHandle.h"
// // #include "eckit/config/Resource.h"

#include "eckit/io/rados/RadosNamespace.h"
#include "eckit/io/rados/RadosPool.h"

#include "fdb5/rados/RadosFieldLocation.h"
#include "fdb5/rados/RadosStore.h"
#include "fdb5/rules/Rule.h"

using namespace eckit;

// #include "eckit/log/Timer.h"
// #include "eckit/log/Bytes.h"

// #include "eckit/io/EmptyHandle.h"
// #include "eckit/io/rados/RadosMultiObjWriteHandle.h"

// #include "fdb5/LibFdb5.h"
// #include "fdb5/rules/Rule.h"
// #include "fdb5/database/FieldLocation.h"
// #include "fdb5/io/FDBFileHandle.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static StoreBuilder<RadosStore> builder("rados");

RadosStore::RadosStore(const Schema& schema, const Key& key, const Config& config) :
    Store(schema), RadosCommon(config, "store", key), config_(config) {

    parseConfig(config_);
}

eckit::URI RadosStore::uri() const {

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    return eckit::RadosNamespace(pool_, db_namespace_).uri();

#else

    return eckit::RadosPool(db_pool_).uri();

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

std::vector<eckit::URI> RadosStore::collocatedDataURIs() const {

    std::vector<eckit::URI> store_unit_uris;

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

        store_unit_uris.push_back(obj.uri());
    }

    return store_unit_uris;
}

std::set<eckit::URI> RadosStore::asCollocatedDataURIs(const std::vector<eckit::URI>& uris) const {

    std::set<eckit::URI> res;

    /// @note: this is only uniquefying the input uris (coming from an index)
    ///   in case theres any duplicate.
    for (auto& uri : uris) {
        res.insert(uri);
    }

    return res;
}

bool RadosStore::exists() const {
    return true;
}

eckit::DataHandle* RadosStore::retrieve(Field& field, Key& remapKey) const {
    return remapKey.empty() ? field.dataHandle() : field.dataHandle(remapKey);

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

    return eckit::RadosNamespace(pool_, db_namespace_).exists();

#else

    return eckit::RadosNamespace(db_pool_, namespace_).exists();

#endif
}

/// @todo: never used in actual fdb-read?
eckit::DataHandle* RadosStore::retrieve(Field& field) const {

    return field.dataHandle();

    // return remapKey.empty() ?
    //     field.dataHandle() :
    //     field.dataHandle(remapKey);
}

std::unique_ptr<const FieldLocation> RadosStore::archive(const uint32_t, const Key& key, const void* data,
                                                         eckit::Length length) {
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


    return std::unique_ptr<RadosFieldLocation>(new RadosFieldLocation(o.uri(), 0, length, fdb5::Key(nullptr, true)));

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

    long len = dh.write(data, length);
    long len = h.write(data, length);

    ASSERT(len == length);

    return std::unique_ptr<RadosFieldLocation>(
        new RadosFieldLocation(o.uri(), offset, length, fdb5::Key(nullptr, true)));

#endif
}

void RadosStore::flush() {
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

    eckit::PathName path = uri.path();
    if (path.isDir()) {
        logVerbose << "rmdir: ";
        logAlways << path << std::endl;
        if (doit) {
            path.rmdir(false);
        }
    }
    else {
        logVerbose << "Unlinking: ";
        logAlways << path << std::endl;
        if (doit) {
            path.unlink(false);
        }

        const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
        const auto n = parts.size();

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

        ASSERT(n == 2 || n == 3);

        ASSERT(parts[0] == pool_);
        ASSERT(parts[1] == db_namespace_);

        if (n == 2) {  // namespace

            eckit::RadosNamespace ns{uri};

            logVerbose << "destroy Rados namespace: " << ns.str() << std::endl;

            if (doit) {
                ns.destroy();  /// @todo: ensureDestroyed?
            }
        }
        else {  // object

            eckit::RadosObject obj{uri};

            logVerbose << "destroy Rados object: " << obj.str() << std::endl;

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

            logVerbose << "destroy Rados pool: " << pool.name() << std::endl;

            if (doit) {
                pool.ensureDestroyed();
            }
        }
        else {  // object

            ASSERT(parts[1] == "default");

            eckit::RadosObject obj{uri};

            logVerbose << "destroy Rados object: " << obj.str() << std::endl;

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

    void RadosStore::print(std::ostream & out) const {

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL

        out << "RadosStore(" << pool_ << "/" << db_namespace_ << ")";

#else

        out << "RadosStore(" << db_pool_ << "/" << namespace_ << ")";

#endif
    }

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

        while (::access(name.c_str(), F_OK) == 0) {
            std::ostringstream os;
            os << eckit::TimeStamp(format) << '.' << hostname << '.' << n++;
            name = os.str();
        }
    }

    eckit::DataHandle* RadosStore::getCachedHandle(const eckit::PathName& path) const {
        HandleStore::const_iterator j = handles_.find(path);
        if (j != handles_.end()) {
            return j->second;
        }
        else {
            return nullptr;
        }

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

        ObjectStore::const_iterator j = dataObjects_.find(key);

        if (j != dataObjects_.end()) {
            return j->second;
        }

        // eckit::RadosObject dataObject = generateDataObject(key);

        dataObjects_.insert(std::pair(key, generateDataObject(key)));

        return dataObjects_.find(key)->second;
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

            for (HandleStore::iterator j = handles_.begin(); j != handles_.end(); ++j) {
                eckit::DataHandle* dh = j->second;
                dh->close();
                delete dh;
            }

            handles_.clear();
        }

        eckit::DataHandle* RadosStore::createFileHandle(const eckit::PathName& path) {

            //    static size_t sizeBuffer = eckit::Resource<unsigned long>("fdbBufferSize", 64 * 1024 * 1024);

            LOG_DEBUG_LIB(LibFdb5) << "Creating RadosWriteHandle to "
                                   << path
                                   //                                 << " with buffer of " << eckit::Bytes(sizeBuffer)
                                   << std::endl;

            return new RadosWriteHandle(path, 0);
        }

        eckit::DataHandle* RadosStore::createAsyncHandle(const eckit::PathName& path) {
            NOTIMP;

            /*    static size_t nbBuffers  = eckit::Resource<unsigned long>("fdbNbAsyncBuffers", 4);
                static size_t sizeBuffer = eckit::Resource<unsigned long>("fdbSizeAsyncBuffer", 64 * 1024 * 1024);

                return new eckit::AIOHandle(path, nbBuffers, sizeBuffer);*/
        }

        eckit::DataHandle* RadosStore::createDataHandle(const eckit::PathName& path) {

            static bool fdbWriteToNull = eckit::Resource<bool>("fdbWriteToNull;$FDB_WRITE_TO_NULL", false);
            if (fdbWriteToNull) {
                return new eckit::EmptyHandle();
            }

            static bool fdbAsyncWrite = eckit::Resource<bool>("fdbAsyncWrite;$FDB_ASYNC_WRITE", false);
            if (fdbAsyncWrite) {
                return createAsyncHandle(path);
            }

            return createFileHandle(path);
        }

        eckit::DataHandle& RadosStore::getDataHandle(const eckit::PathName& path) {
            eckit::DataHandle* dh = getCachedHandle(path);
            if (!dh) {
                dh = createDataHandle(path);
                ASSERT(dh);
                handles_[path] = dh;
                dh->openForWrite(0);
            }
            return *dh;
        }

        eckit::PathName RadosStore::generateDataPath(const Key& key) const {

            eckit::PathName dpath(directory_);
            dpath /= key.valuesToString();
            dpath = eckit::PathName::unique(dpath) + ".data";
            return dpath;
        }

        eckit::PathName RadosStore::getDataPath(const Key& key) {
            PathStore::const_iterator j = dataPaths_.find(key);
            if (j != dataPaths_.end()) {
                return j->second;
            }

            eckit::PathName dataPath = generateDataPath(key);

            dataPaths_[key] = dataPath;

            return dataPath;
            dataObjects_.clear();
        }

        void RadosStore::flushDataHandles() {

            for (HandleStore::iterator j = handles_.begin(); j != handles_.end(); ++j) {
                eckit::DataHandle* dh = j->second;
                dh->flush();
            }
        }

        void RadosStore::print(std::ostream & out) const {
            out << "RadosStore(" << directory_ << ")";
        }

        static StoreBuilder<RadosStore> builder("rados");
#endif

        // eckit::DataHandle *RadosStore::createAsyncHandle(const eckit::PathName &path) {
        //     NOTIMP;

        // /*    static size_t nbBuffers  = eckit::Resource<unsigned long>("fdbNbAsyncBuffers", 4);
        //     static size_t sizeBuffer = eckit::Resource<unsigned long>("fdbSizeAsyncBuffer", 64 * 1024 * 1024);

        //     return new eckit::AIOHandle(path, nbBuffers, sizeBuffer);*/
        // }

        // eckit::DataHandle *RadosStore::createDataHandle(const eckit::PathName &path) {

        //     static bool fdbWriteToNull = eckit::Resource<bool>("fdbWriteToNull;$FDB_WRITE_TO_NULL", false);
        //     if(fdbWriteToNull)
        //         return new eckit::EmptyHandle();

        //     static bool fdbAsyncWrite = eckit::Resource<bool>("fdbAsyncWrite;$FDB_ASYNC_WRITE", false);
        //     if(fdbAsyncWrite)
        //         return createAsyncHandle(path);

        //     return new RadosMultiObjWriteHandle(path, 0);
        // }

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

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include "eckit/log/Timer.h"
// #include "eckit/log/Bytes.h"

#include "eckit/config/Resource.h"
// #include "eckit/io/EmptyHandle.h"
// #include "eckit/io/rados/RadosWriteHandle.h"

// #include "fdb5/LibFdb5.h"
// #include "fdb5/rules/Rule.h"
// #include "fdb5/database/FieldLocation.h"
#include "fdb5/daos/DaosFieldLocation.h"
#include "fdb5/daos/DaosStore.h"
#include "fdb5/daos/DaosHandle.h"
#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosException.h"
// #include "fdb5/io/FDBFileHandle.h"

// using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosStore::DaosStore(const Schema& schema, const Key& key, const Config& config) :
    Store(schema), config_(config), db_str_(key.valuesToString()) {

    pool_ = config_.getSubConfiguration("daos").getSubConfiguration("store").getString("pool", "default");

    pool_ = eckit::Resource<std::string>("fdbDaosStorePool;$FDB_DAOS_STORE_POOL", pool_);

    fdb5::DaosManager::instance().configure(config_.getSubConfiguration("daos").getSubConfiguration("client"));

    /// @todo: should assert the store actually exists, as in the constructors of DaosPool etc.

}

DaosStore::DaosStore(const Schema& schema, const eckit::URI& uri, const Config& config) :
    Store(schema), config_(config) {

    eckit::PathName db_path(uri.path());

    eckit::Tokenizer t("/");
    std::vector<std::string> parts = t.tokenize(db_path.asString());
    ASSERT(parts.size() == 3);

    pool_ = db_path.dirName().baseName().asString();
    
    db_str_ = db_path.baseName().asString();

    fdb5::DaosManager::instance().configure(config_.getSubConfiguration("daos").getSubConfiguration("client"));

}

eckit::URI DaosStore::uri() const {

    return eckit::URI("daos", pool_);
    
}

bool DaosStore::uriBelongs(const eckit::URI& uri) const {

    /// @todo: assert uri points to a (not necessarily existing) array object
    return (
        (uri.scheme() == type()) && 
        (uri.path().dirName().baseName().asString().rfind("store_" + db_str_ + "_", 0) == 0));

}

bool DaosStore::uriExists(const eckit::URI& uri) const {

    /// @todo: revisit the name of this method. uriExists suggests this method can be used to check e.g. if a DAOS object pointed
    //   to by an URI exists. However this is just meant to check if a Store container pointed to by a URI exists.
    ASSERT(uri.scheme() == type());
    eckit::PathName p(uri.path());
    // ensure provided URI pointing to a Store container
    ASSERT(p.dirName().asString() == eckit::PathName(pool_).asString());
    std::string cont_name = p.baseName();
    ASSERT(cont_name.rfind("store_", 0) == 0);

    try {
        fdb5::DaosSession s{};
        fdb5::DaosPool& p = s.getPool(pool_);
        fdb5::DaosContainer& c = p.getContainer(cont_name);
    } catch (const fdb5::DaosEntityNotFoundException& e) {
        return false;
    }
    return true;

}

eckit::PathName DaosStore::getStoreUnitPath(const eckit::URI& uri) const {

    ASSERT(uri.scheme() == type());
    std::string s(uri.name());
    eckit::Tokenizer t("/");
    std::vector<std::string> parts = t.tokenize(s);
    ASSERT(parts.size() == 3);

    return uri.path().dirName();

}


std::vector<eckit::URI> DaosStore::storeUnitURIs() const {

    fdb5::DaosSession s{};
    fdb5::DaosPool& p = s.getPool(pool_);
    std::vector<std::string> cont_labels = p.listContainers();

    std::vector<eckit::URI> store_unit_uris;
    
    for (const auto& label : cont_labels) {
        
        if (label.rfind("store_" + db_str_ + "_", 0) == 0)
            store_unit_uris.push_back(eckit::URI(type(), eckit::PathName(pool_) / label));

    }

    return store_unit_uris;

}

bool DaosStore::exists() const {

    try {
        fdb5::DaosSession s{};
        fdb5::DaosPool& p = s.getPool(pool_);
    } catch (const fdb5::DaosEntityNotFoundException& e) {
        return false;
    }
    return true;

}

/// @todo: never used in actual fdb-read?
eckit::DataHandle* DaosStore::retrieve(Field& field) const {

    return field.dataHandle();

}
//     return remapKey.empty() ?
//         field.dataHandle() :
//         field.dataHandle(remapKey);
// }

FieldLocation* DaosStore::archive(const Key &key, const void *data, eckit::Length length) {

    dirty_ = true;

    fdb5::DaosSession s{};
    fdb5::DaosPool& p = s.getPool(pool_);
    fdb5::DaosContainer& c = p.ensureContainer("store_" + db_str_ + "_" + key.valuesToString());
    fdb5::DaosObject o = c.createObject();
    
    eckit::URI uri = o.URI();

    fdb5::DaosHandle dh(std::move(o));
    eckit::Offset position = dh.position();
    dh.openForWrite(length);
    {
        eckit::AutoClose closer(dh);
        long len = dh.write(data, length);
        ASSERT(len == length);
    }

    return new DaosFieldLocation(uri, position, length, fdb5::Key());

}

void DaosStore::flush() {

    // if (!dirty_) {
    //     return;
    // }

    // // ensure consistent state before writing Toc entry

    // flushDataHandles();

    dirty_ = false;

}

void DaosStore::close() {

    // closeDataHandles();

}

void DaosStore::remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const {

    ASSERT(uri.scheme() == type());

    eckit::Tokenizer t("/");
    std::vector<std::string> parts = t.tokenize(uri.name());
    
    ASSERT(parts.size() > 1);
    ASSERT(parts[0] == pool_);
    if (parts.size() > 2) NOTIMP;

    /// @todo: should try/catch? no, should check cont exists
    fdb5::DaosSession s{};
    fdb5::DaosPool& p = s.getPool(pool_);
    fdb5::DaosContainer& c = p.getContainer(parts[1]);

    logVerbose << "destroy container: ";
    logAlways << c.name() << std::endl;
    if (doit) c.destroy();

}

// eckit::DataHandle *RadosStore::getCachedHandle( const eckit::PathName &path ) const {
//     HandleStore::const_iterator j = handles_.find( path );
//     if ( j != handles_.end() )
//         return j->second;
//     else
//         return nullptr;
// }

// void RadosStore::closeDataHandles() {
//     for ( HandleStore::iterator j = handles_.begin(); j != handles_.end(); ++j ) {
//         eckit::DataHandle *dh = j->second;
//         dh->close();
//         delete dh;
//     }
//     handles_.clear();
// }

// eckit::DataHandle *RadosStore::createFileHandle(const eckit::PathName &path) {

// //    static size_t sizeBuffer = eckit::Resource<unsigned long>("fdbBufferSize", 64 * 1024 * 1024);

//     eckit::Log::debug<LibFdb5>() << "Creating RadosWriteHandle to " << path
// //                                 << " with buffer of " << eckit::Bytes(sizeBuffer)
//                                  << std::endl;

//     return new RadosWriteHandle(path, 0);
// }

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

//     return createFileHandle(path);
// }

// eckit::DataHandle& RadosStore::getDataHandle( const eckit::PathName &path ) {
//     eckit::DataHandle *dh = getCachedHandle(path);
//     if ( !dh ) {
//         dh = createDataHandle(path);
//         ASSERT(dh);
//         handles_[path] = dh;
//         dh->openForWrite(0);
//     }
//     return *dh;
// }

// eckit::PathName RadosStore::generateDataPath(const Key &key) const {

//     eckit::PathName dpath ( directory_ );
//     dpath /=  key.valuesToString();
//     dpath = eckit::PathName::unique(dpath) + ".data";
//     return dpath;

// }

// eckit::PathName DaosStore::getDataPath(const Key &key) {
//     PathStore::const_iterator j = dataPaths_.find(key);
//     if ( j != dataPaths_.end() )
//         return j->second;

    // eckit::PathName dataPath = generateDataPath(key);

//     dataPaths_[ key ] = dataPath;

//     return dataPath;

// }

// void RadosStore::flushDataHandles() {

//     for (HandleStore::iterator j = handles_.begin(); j != handles_.end(); ++j) {
//         eckit::DataHandle *dh = j->second;
//         dh->flush();
//     }
// }

void DaosStore::print(std::ostream &out) const {

    out << "DaosStore(" << pool_ << ")";

}

static StoreBuilder<DaosStore> builder("daos");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

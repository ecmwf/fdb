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
#include "fdb5/daos/DaosArrayHandle.h"
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

    pool_ = "default";

    eckit::LocalConfiguration c{};

    if (config_.has("daos")) c = config_.getSubConfiguration("daos");
    if (c.has("store")) pool_ = c.getSubConfiguration("store").getString("pool", pool_);

    pool_ = eckit::Resource<std::string>("fdbDaosStorePool;$FDB_DAOS_STORE_POOL", pool_);

    if (c.has("client"))
        fdb5::DaosManager::instance().configure(c.getSubConfiguration("client"));

    /// @todo: should assert the store actually exists, as in the constructors of DaosPool etc.

}

DaosStore::DaosStore(const Schema& schema, const eckit::URI& uri, const Config& config) :
    Store(schema), config_(config) {

    fdb5::DaosArrayName db_name(uri);
    ASSERT(db_name.hasOID());

    pool_ = db_name.poolName();
    
    db_str_ = db_name.contName();

    eckit::LocalConfiguration c{};
    if (c.has("daos")) c = c.getSubConfiguration("daos");
    if (c.has("client"))
        fdb5::DaosManager::instance().configure(c.getSubConfiguration("client"));

}

eckit::URI DaosStore::uri() const {

    return eckit::URI(type(), pool_);
    
}

bool DaosStore::uriBelongs(const eckit::URI& uri) const {

    /// @todo: assert uri points to a (not necessarily existing) array object
    return (
        (uri.scheme() == type()) && 
        (fdb5::DaosName(uri).contName().rfind("store_" + db_str_ + "_", 0) == 0));

}

bool DaosStore::uriExists(const eckit::URI& uri) const {

    /// @todo: revisit the name of this method. uriExists suggests this method can be used to check e.g. if a DAOS object pointed
    //   to by an URI exists. However this is just meant to check if a Store container pointed to by a URI exists.
    ASSERT(uri.scheme() == type());
    fdb5::DaosName n(uri);
    ASSERT(n.hasContName());
    // ensure provided URI pointing to a Store container
    ASSERT(n.poolName() == pool_);
    ASSERT(n.contName().rfind("store_", 0) == 0);

    return fdb5::DaosName{pool_, n.contName()}.exists();

}

// TODO: transform to getStoreunitURI
eckit::PathName DaosStore::getStoreUnitPath(const eckit::URI& uri) const {

    ASSERT(uri.scheme() == type());
    fdb5::DaosName n(uri);
    ASSERT(n.hasOID());

    return uri.path().dirName();

}

std::vector<eckit::URI> DaosStore::storeUnitURIs() const {

    // TODO: implement DaosName::listContainers or listContainerNames
    fdb5::DaosSession s{};
    fdb5::DaosPool& p = s.getPool(pool_);
    std::vector<std::string> cont_labels = p.listContainers();

    std::vector<eckit::URI> store_unit_uris;
    
    for (const auto& label : cont_labels) {
        
        if (label.rfind("store_" + db_str_ + "_", 0) == 0)
            store_unit_uris.push_back(fdb5::DaosName(pool_, label).URI());

    }

    return store_unit_uris;

}

bool DaosStore::exists() const {

    return fdb5::DaosName(pool_).exists();

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

    fdb5::DaosArrayName n = fdb5::DaosName(pool_, "store_" + db_str_ + "_" + key.valuesToString()).createArrayName(); // TODO: pass oclass from config
    n.generateOID();
    std::unique_ptr<eckit::DataHandle> h(n.dataHandle());
    h->openForWrite(length);
    eckit::AutoClose closer(*h);
    h->write(data, length);

    return new DaosFieldLocation(n.URI(), 0, length, fdb5::Key());

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

    fdb5::DaosName n{uri};
    
    ASSERT(n.hasContName());
    ASSERT(n.poolName() == pool_);
    if (n.hasOID()) NOTIMP;

    logVerbose << "destroy container: ";
    logAlways << n.asString() << std::endl;
    if (doit) n.destroy();

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

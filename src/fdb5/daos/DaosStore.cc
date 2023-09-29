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

#include "fdb5/daos/DaosFieldLocation.h"
#include "fdb5/daos/DaosStore.h"
#include "fdb5/daos/DaosArrayHandle.h"
#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosException.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosStore::DaosStore(const Schema& schema, const Key& key, const Config& config) :
    Store(schema), DaosCommon(config, "store", key), config_(config), db_str_(db_cont_) {

    /// @todo: should assert the store actually exists, as in the constructors of DaosPool etc.

}

DaosStore::DaosStore(const Schema& schema, const eckit::URI& uri, const Config& config) :
    Store(schema), DaosCommon(config, "store", uri), config_(config), db_str_(db_cont_) {

}

eckit::URI DaosStore::uri() const {

    return eckit::URI(type(), pool_);
    
}

bool DaosStore::uriBelongs(const eckit::URI& uri) const {

    /// @todo: avoid building a DaosName as it makes uriBelongs expensive
    /// @todo: assert uri points to a (not necessarily existing) array object
    return (
        (uri.scheme() == type()) && 
        (fdb5::DaosName(uri).contName().rfind(db_str_, 0) == 0));
        //(fdb5::DaosName(uri).contName().rfind("store_" + db_str_, 0) == 0));
        //(fdb5::DaosName(uri).contName().rfind("store_" + db_str_ + "_", 0) == 0));

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

std::vector<eckit::URI> DaosStore::storeUnitURIs() const {

    fdb5::DaosSession s{};
    fdb5::DaosPool& p = s.getPool(pool_);
    std::vector<std::string> cont_labels = p.listContainers();

    std::vector<eckit::URI> store_unit_uris;
    
    for (const auto& label : cont_labels) {
        
        //if (label.rfind("store_" + db_str_ + "_", 0) == 0)
        //if (label.rfind("store_" + db_str_, 0) == 0)
        if (label.rfind(db_str_, 0) == 0)
            store_unit_uris.push_back(fdb5::DaosName(pool_, label).URI());

    }

    return store_unit_uris;

}

std::set<eckit::URI> DaosStore::asStoreUnitURIs(const std::vector<eckit::URI>& uris) const {

    std::set<eckit::URI> res;

    for (auto& uri : uris) {

        fdb5::DaosName n{uri};
        res.insert(fdb5::DaosName(n.poolName(), n.contName()).URI());

    }

    return res;

}

bool DaosStore::exists() const {

    return fdb5::DaosName(pool_).exists();

}

/// @todo: never used in actual fdb-read?
eckit::DataHandle* DaosStore::retrieve(Field& field) const {

    return field.dataHandle();

}

FieldLocation* DaosStore::archive(const Key &key, const void *data, eckit::Length length) {

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

    //fdb5::DaosArrayName n = fdb5::DaosName(pool_, "store_" + db_str_ + "_" + key.valuesToString()).createArrayName(); // TODO: pass oclass from config
    //fdb5::DaosArrayName n = fdb5::DaosName(pool_, "store_" + db_str_).createArrayName(); // TODO: pass oclass from config
    /// @note: performed RPCs:
    /// - open pool if not cached (daos_pool_connect) -- always skipped as it is cached after selectDatabase.
    ///   If the cat backend is toc, then it is performed but only on first write.
    /// - check if container exists if not cached (daos_cont_open) -- always skipped as it is cached after selectDatabase.
    ///   If the cat backend is toc, then it is performed but only on first write.
    /// - allocate oid (daos_cont_alloc_oids) -- skipped most of the times as oids per alloc is set to 100
    fdb5::StatsTimer st{"archive 08 array alloc oid", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
    fdb5::DaosArrayName n = fdb5::DaosName(pool_, db_str_).createArrayName(); // TODO: pass oclass from config
    st.stop();

    /// @note: performed RPCs:
    /// - daos_obj_generate_oid -- always performed
    st.start("archive 09 array generate oid", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
    n.generateOID();
    st.stop();

    std::unique_ptr<eckit::DataHandle> h(n.dataHandle());

    /// @note: performed RPCs:
    /// - open pool if not cached (daos_pool_connect) -- always skipped, opened above
    /// - check if container exists if not cached/open (daos_cont_open) -- always skipped, cached/open above
    /// - generate array oid (daos_obj_generate_oid) -- always skipped, already generated above
    /// - open container if not open (daos_cont_open) -- always skipped
    /// - generate oid again (daos_obj_generate_oid) -- always skipped
    /// - create (daos_array_create) -- always performed
    /// - open (daos_array_open) -- always skipped, create already opens
    h->openForWrite(length);
    eckit::AutoClose closer(*h);

    /// @note: performed RPCs:
    /// - write (daos_array_write) -- always performed
    h->write(data, length);

    return new DaosFieldLocation(n.URI(), 0, length, fdb5::Key());

    /// @note: performed RPCs:
    /// - close (daos_array_close here) -- always performed

}

void DaosStore::flush() {}

void DaosStore::remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const {

    fdb5::DaosName n{uri};
    
    ASSERT(n.hasContName());
    ASSERT(n.poolName() == pool_);
    if (n.hasOID()) NOTIMP;

    logVerbose << "destroy container: ";
    logAlways << n.asString() << std::endl;
    if (doit) n.destroy();

}

void DaosStore::print(std::ostream &out) const {

    out << "DaosStore(" << pool_ << ")";

}

static StoreBuilder<DaosStore> builder("daos");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

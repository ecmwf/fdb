/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"

#include "fdb5/database/WipeState.h"

#include "fdb5/daos/DaosArrayHandle.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosException.h"
#include "fdb5/daos/DaosFieldLocation.h"
#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosStore.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosStore::DaosStore(const Key& key, const Config& config) :
    Store(), DaosCommon(config, "store", key), archivedFields_(0) {}

DaosStore::DaosStore(const eckit::URI& uri, const Config& config) :
    Store(), DaosCommon(config, "store", uri), archivedFields_(0) {}

eckit::URI DaosStore::uri() const {

    return fdb5::DaosName(pool_, db_cont_).URI();
}

eckit::URI DaosStore::uri(const eckit::URI& dataURI) {
    ASSERT(dataURI.scheme() == "daos");
    fdb5::DaosName n(dataURI);
    return fdb5::DaosName{n.poolName(), n.containerName()}.URI();
}

bool DaosStore::uriBelongs(const eckit::URI& uri) const {

    fdb5::DaosName n{uri};

    bool result = (uri.scheme() == type());
    result = result && (n.poolName() == pool_);
    result = result && (n.containerName().rfind(db_cont_, 0) == 0);
    result = result && (n.OID().otype() == DAOS_OT_ARRAY || n.OID().otype() == DAOS_OT_ARRAY_BYTE);

    return result;
}

bool DaosStore::uriExists(const eckit::URI& uri) const {

    /// @todo: revisit the name of this method

    ASSERT(uri.scheme() == type());
    fdb5::DaosName n(uri);
    ASSERT(n.hasContainerName());
    ASSERT(n.poolName() == pool_);
    ASSERT(n.containerName() == db_cont_);
    ASSERT(n.hasOID());

    return n.exists();
}

std::set<eckit::URI> DaosStore::collocatedDataURIs() const {

    std::set<eckit::URI> collocated_data_uris;

    fdb5::DaosName db_cont{pool_, db_cont_};

    if (!db_cont.exists())
        return collocated_data_uris;

    for (const auto& oid : db_cont.listOIDs()) {

        if (oid.otype() != DAOS_OT_KV_HASHED && oid.otype() != DAOS_OT_ARRAY_BYTE)
            throw eckit::SeriousBug("Found non-KV non-ByteArray objects in DB container " + db_cont.URI().asString());

        if (oid.otype() == DAOS_OT_KV_HASHED)
            continue;

        collocated_data_uris.insert(fdb5::DaosArrayName(pool_, db_cont_, oid).URI());
    }

    return collocated_data_uris;
}

std::set<eckit::URI> DaosStore::asCollocatedDataURIs(const std::set<eckit::URI>& uris) const {

    std::set<eckit::URI> res;

    for (auto& uri : uris)
        /// @note: seems redundant, but intends to check validity of input URIs
        res.insert(fdb5::DaosName(uri).URI());

    return res;
}

bool DaosStore::exists() const {

    return fdb5::DaosName(pool_).exists();
}

/// @todo: never used in actual fdb-read?
eckit::DataHandle* DaosStore::retrieve(Field& field) const {

    return field.dataHandle();
}

std::unique_ptr<const FieldLocation> DaosStore::archive(const Key&, const void* data, eckit::Length length) {

    /// @note: performed RPCs:
    /// - open pool if not cached (daos_pool_connect) -- always skipped as it is cached after selectDatabase.
    ///   If the cat backend is toc, then it is performed but only on first write.
    /// - check if container exists if not cached (daos_cont_open) -- always skipped as it is cached after
    /// selectDatabase.
    ///   If the cat backend is toc, then it is performed but only on first write.
    /// - allocate oid (daos_cont_alloc_oids) -- skipped most of the times as oids per alloc is set to 100
    fdb5::DaosArrayName n =
        fdb5::DaosName(pool_, db_cont_).createArrayName(OC_S1, false);  // TODO: pass oclass from config

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

    archivedFields_++;

    return std::make_unique<const DaosFieldLocation>(n.URI(), 0, length, fdb5::Key{});

    /// @note: performed RPCs:
    /// - close (daos_array_close here) -- always performed
}

size_t DaosStore::flush() {
    size_t archived = archivedFields_;
    archivedFields_ = 0;
    return archived;
}

void DaosStore::remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const {

    fdb5::DaosName n{uri};

    ASSERT(n.hasContainerName());
    ASSERT(n.poolName() == pool_);
    ASSERT(n.containerName() == db_cont_);

    if (n.hasOID()) {
        ASSERT(n.OID().otype() == DAOS_OT_ARRAY_BYTE);
        logVerbose << "destroy array: ";
    }
    else {
        logVerbose << "destroy container: ";
    }

    logAlways << n.asString() << std::endl;
    if (doit)
        n.destroy();
}

void DaosStore::prepareWipe(StoreWipeState& storeState, bool doit, bool unsafeWipeAll) {
    // Note: doit and unsafeWipeAll do not affect the preparation of a local daos store wipe.

    const std::set<eckit::URI>& dataURIs = storeState.includedDataURIs();  // included according to cat
    const std::set<eckit::URI>& safeURIs = storeState.safeURIs();          // excluded according to cat

    std::set<eckit::URI> nonExistingURIs;
    for (auto& uri : dataURIs) {

        /// @note: uncomment if gribjump on daos is ever implemented
        // for (const auto& aux : getAuxiliaryURIs(uri, true)) {
        //     storeState.insertAuxiliaryURI(aux);
        // }

        // URI may not exist (e.g. due to a prior incomplete wipe.)
        if (!fdb5::DaosName{uri}.exists()) {
            nonExistingURIs.insert(uri);
        }
    }

    for (const auto& uri : nonExistingURIs) {
        storeState.unincludeURI(uri);
    }

    bool all = safeURIs.empty();
    if (!all) {
        return;
    }

    // Plan to erase all objects in this container. Find any unaccounted for.
    // const auto& auxURIs = storeState.dataAuxiliaryURIs();
    const fdb5::DaosKeyValueName& db_kv = dbKeyValue();

    fdb5::DaosName cont{db_kv.poolName(), db_kv.containerName()};
    std::vector<fdb5::DaosOID> allOIDs = cont.listOIDs();
    for (const fdb5::DaosOID& oid : allOIDs) {
        auto uri = fdb5::DaosName{cont.poolName(), cont.containerName(), oid}.URI();
        if (dataURIs.find(uri) == dataURIs.end() && safeURIs.find(uri) == safeURIs.end()) {
            // auxURIs.find(u) == auxURIs.end() &&
            storeState.insertUnrecognised(uri);
        }
    }

}

bool DaosStore::doWipeUnknownContents(const std::set<eckit::URI>& unknownURIs) const {
    for (const auto& uri : unknownURIs) {
        fdb5::DaosName name{uri};
        ASSERT(name.hasOID());
        if (name.exists()) {
            /// @todo: DaosCatalogue::remove takes a DaosName as input instead of a URI. Homogenise.
            /// @todo: check if TocStore still prints to stdout in latest develop
            remove(uri, std::cout, std::cout, true);
        }
    }

    return true;
}

bool DaosStore::doWipe(const StoreWipeState& wipeState) const {
    bool wipeAll = wipeState.safeURIs().empty();

    // for (const auto& uri : wipeState.dataAuxiliaryURIs()) {
    //     remove(uri, std::cout, std::cout, true);
    // }

    for (const auto& uri : wipeState.includedDataURIs()) {
        fdb5::DaosName name{uri};
        ASSERT(name.hasOID());
        remove(uri, std::cout, std::cout, true);
    }

    if (wipeAll) {
        fdb5::DaosName cont{pool_, db_cont_};
        emptyDatabases_.insert(cont.URI());
    }

    return true;
}

void DaosStore::doWipeEmptyDatabases() const {

    if (emptyDatabases_.size() == 0) return;

    ASSERT(emptyDatabases_.size() == 1);

    // remove the database container
    fdb5::DaosName contName{*(emptyDatabases_.begin())};
    ASSERT(!contName.hasOID());
    if (contName.exists()) {
        ASSERT(contName.listOIDs().size() == 0);
        remove(contName.URI(), std::cout, std::cout, true);
    }

    emptyDatabases_.clear();
}

bool DaosStore::doUnsafeFullWipe() const {

    /// @note: if the database container also holds a catalogue, the wiping is skipped
    /// as the catalogue is in charge.
    /// @note: objects always exist in DAOS. The presence of a 'key' key in the database key-value
    /// is used to determine whether a catalogue exists in the container.
    if (!db_kv_->has("key")) {
        // remove the database container
        fdb5::DaosName contName{pool_, db_cont_};
        ASSERT(!contName.hasOID());
        if (contName.exists()) {
            remove(contName.URI(), std::cout, std::cout, true);
        }
    }

    return true;
}

void DaosStore::print(std::ostream& out) const {

    out << "DaosStore(" << pool_ << ")";
}

static StoreBuilder<DaosStore> builder("daos");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

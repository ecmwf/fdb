/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include "eckit/config/Resource.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/io/rados/RadosException.h"

// #include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/database/DatabaseNotFoundException.h"

#include "fdb5/rados/RadosCatalogue.h"
// #include "fdb5/daos/DaosName.h"
// #include "fdb5/daos/DaosSession.h"
#include "fdb5/rados/RadosIndex.h"
// #include "fdb5/daos/DaosWipeVisitor.h"

// using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RadosCatalogue::RadosCatalogue(const Key& key, const fdb5::Config& config) :
    Catalogue(key, ControlIdentifiers{}, config), RadosCommon(config, "catalogue", key) {

    // TODO: apply the mechanism in RootManager::directory, using
    //   FileSpaceTables to determine root_pool_name_ according to key
    //   and using DbPathNamerTables to determine db_cont_name_ according
    //   to key

}

RadosCatalogue::RadosCatalogue(const eckit::URI& uri, const ControlIdentifiers& controlIdentifiers, const fdb5::Config& config) :
    Catalogue(Key(), controlIdentifiers, config), RadosCommon(config, "catalogue", uri) {

#ifdef fdb5_HAVE_RADOS_BACKENDS_SINGLE_POOL
    std::string pool = pool_;
    std::string nspace = db_namespace_;
#else
    std::string pool = db_pool_;
    std::string nspace = namespace_;
#endif

    // Read the real DB key into the DB base object
    try {

        std::vector<char> data;
        eckit::MemoryStream ms = db_kv_->getMemoryStream(data, "key", "DB kv");
        dbKey_ = fdb5::Key(ms);

    } catch (eckit::RadosEntityNotFoundException& e) {

        throw fdb5::DatabaseNotFoundException(
            std::string("RadosCatalogue database not found ") +
            "(pool: '" + pool + "', namespace: '" + nspace + "')"
        );

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

void RadosCatalogue::loadSchema() {

    eckit::Timer timer("RadosCatalogue::loadSchema()", eckit::Log::debug<fdb5::LibFdb5>());

    /// @note: performed RPCs:
    /// - daos_obj_generate_oid
    /// - daos_kv_open
    /// - daos_kv_get without a buffer
    /// - daos_kv_get
    std::vector<char> data;
    db_kv_->getMemoryStream(data, "schema", "DB Key-Value");

    std::istringstream stream{std::string(data.begin(), data.end())};
    schema_.load(stream);

}

WipeVisitor* RadosCatalogue::wipeVisitor(const Store& store, const metkit::mars::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const {
    NOTIMP;
    // return new RadosWipeVisitor(*this, store, request, out, doit, porcelain, unsafeWipeAll);
}

std::vector<Index> RadosCatalogue::indexes(bool) const {

    /// @note: sorted is not implemented as is not necessary in this backend.

    /// @note: performed RPCs:
    /// - db kv open (daos_kv_open)
    /// - db kv list keys (daos_kv_list)

    std::vector<fdb5::Index> res;

    for (const auto& key : db_kv_->keys()) {

        /// @todo: document these well. Single source these reserved values.
        ///    Ensure where appropriate that user-provided keys do not collide.
        if (key == "schema" || key == "key") continue;

        /// @note: performed RPCs:
        /// - db kv get index location size (daos_kv_get without a buffer)
        /// - db kv get index location (daos_kv_get)
        std::vector<char> v;
        auto m = db_kv_->getMemoryStream(v, key, "DB kv");

        eckit::URI uri(std::string(v.begin(), v.end()));

        /// @note: performed RPCs:
        /// - index kv open (daos_kv_open)
        /// - index kv get size (daos_kv_get without a buffer)
        /// - index kv get key (daos_kv_get)
        /// @note: the following three lines intend to check whether the index kv exists 
        ///   or not. The DaosKeyValue constructor calls kv open, which always succeeds,
        ///   so it is not useful on its own to check whether the index KV existed or not.
        ///   Instead, presence of a "key" key in the KV is used to determine if the index 
        ///   KV existed.
        eckit::RadosKeyValue index_kv{uri};
        std::optional<fdb5::Key> index_key;
        try {
            std::vector<char> data;
            eckit::MemoryStream ms = index_kv.getMemoryStream(data, "key", "index KV");
            index_key.emplace(ms);
        } catch (eckit::RadosEntityNotFoundException& e) {
            continue; /// @note: the index_kv may not exist after a failed wipe
            /// @todo: the index_kv may exist even if it does not have the "key" key
        }

        res.push_back(Index(new fdb5::RadosIndex(index_key.value(), index_kv, false)));

    }

    return res;
    
}

std::string RadosCatalogue::type() const {

    return RadosCatalogue::catalogueTypeName();
    
}

// void RadosCatalogue::remove(const fdb5::DaosNameBase& n, std::ostream& logAlways, std::ostream& logVerbose, bool doit) {

//     ASSERT(n.hasContainerName());

//     logVerbose << "Removing " << (n.hasOID() ? "KV" : "container") << ": ";
//     logAlways << n.URI() << std::endl;
//     if (doit) n.destroy();

// }

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

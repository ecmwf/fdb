/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <climits>  // for PATH_MAX

#include "eckit/io/MemoryHandle.h"
#include "eckit/serialisation/HandleStream.h"
#include "eckit/serialisation/MemoryStream.h"
#include "fdb5/daos/DaosIndex.h"
#include "fdb5/daos/DaosLazyFieldLocation.h"
#include "fdb5/daos/DaosSession.h"

fdb5::DaosKeyValueName buildIndexKvName(const fdb5::Key& key, const fdb5::DaosName& name) {

    ASSERT(!name.hasOID());

    /// create index kv
    /// @todo: pass oclass from config
    /// @todo: hash string into lower oid bits
    fdb5::DaosKeyValueOID index_kv_oid{key.valuesToString(), OC_S1};

    return fdb5::DaosKeyValueName{name.poolName(), name.containerName(), index_kv_oid};
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosIndex::DaosIndex(const Key& key, const Catalogue& catalogue, const fdb5::DaosName& name) :
    IndexBase(key, "daosKeyValue"), location_(buildIndexKvName(key, name), 0) {

    fdb5::DaosSession s{};

    /// @note: performed RPCs:
    /// - generate index kv oid (daos_obj_generate_oid)
    /// - create/open index kv (daos_kv_open)
    fdb5::DaosKeyValue index_kv_obj{s, location_.daosName()};

    /// write indexKey under "key"
    eckit::MemoryHandle h{(size_t)PATH_MAX};
    eckit::HandleStream hs{h};
    h.openForWrite(eckit::Length(0));
    {
        eckit::AutoClose closer(h);
        hs << key;
    }

    int idx_key_max_len = 512;

    if (hs.bytesWritten() > idx_key_max_len)
        throw eckit::Exception("Serialised index key exceeded configured maximum index key length.");

    /// @note: performed RPCs:
    /// - record index key into index kv (daos_kv_put)
    index_kv_obj.put("key", h.data(), hs.bytesWritten());
}

DaosIndex::DaosIndex(const Key& key, const Catalogue& catalogue, const fdb5::DaosKeyValueName& name, bool readAxes) :
    IndexBase(key, "daosKeyValue"), location_(name, 0) {

    if (readAxes)
        updateAxes();
}

void DaosIndex::updateAxes() {

    fdb5::DaosSession s{};
    const fdb5::DaosKeyValueName& index_kv_name = location_.daosName();

    /// @note: performed RPCs:
    /// - ensure axis kv exists (daos_obj_open)
    fdb5::DaosKeyValue index_kv{s, index_kv_name};

    int axis_names_max_len = 512;  /// @todo: take from config
    std::vector<char> axes_data((long)axis_names_max_len);

    /// @note: performed RPCs:
    /// - get axes key size and content (daos_kv_get without buffer + daos_kv_get)
    long res = index_kv.get("axes", &axes_data[0], axis_names_max_len);

    std::vector<std::string> axis_names;
    eckit::Tokenizer parse(",");
    parse(std::string(axes_data.begin(), std::next(axes_data.begin(), res)), axis_names);
    std::string indexKey{key_.valuesToString()};
    for (const auto& name : axis_names) {
        /// @todo: take oclass from config
        fdb5::DaosKeyValueOID oid(indexKey + std::string{"."} + name, OC_S1);
        fdb5::DaosKeyValueName nkv(index_kv_name.poolName(), index_kv_name.containerName(), oid);

        /// @note: performed RPCs:
        /// - generate axis kv oid (daos_obj_generate_oid)
        /// - ensure axis kv exists (daos_obj_open)
        fdb5::DaosKeyValue axis_kv{s, nkv};

        /// @note: performed RPCs:
        /// - one or more kv list (daos_kv_list)
        axes_.insert(name, axis_kv.keys());
    }

    axes_.sort();
}

bool DaosIndex::get(const Key& key, const Key& remapKey, Field& field) const {

    const fdb5::DaosKeyValueName& n = location_.daosName();

    fdb5::DaosSession s{};

    /// @note: performed RPCs:
    /// - ensure index kv exists (daos_obj_open)
    fdb5::DaosKeyValue index{s, n};

    std::string query{key.valuesToString()};

    int field_loc_max_len = 512;  /// @todo: read from config
    std::vector<char> loc_data((long)field_loc_max_len);
    long res;

    try {

        /// @note: performed RPCs:
        /// - retrieve field array location from index kv (daos_kv_get)
        res = index.get(query, &loc_data[0], field_loc_max_len);
    }
    catch (fdb5::DaosEntityNotFoundException& e) {

        /// @note: performed RPCs:
        /// - close index kv (daos_obj_close)

        return false;
    }

    eckit::MemoryStream ms{&loc_data[0], (size_t)res};

    /// @note: timestamp read for informational purpoes. See note in DaosIndex::add.
    time_t ts;
    ms >> ts;

    auto loc = std::shared_ptr<fdb5::FieldLocation>(eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms));
    field    = fdb5::Field(loc, ts, fdb5::FieldDetails());

    /// @note: performed RPCs:
    /// - close index kv (daos_obj_close)

    return true;
}

void DaosIndex::add(const Key& key, const Field& field) {

    eckit::MemoryHandle h{(size_t)PATH_MAX};
    eckit::HandleStream hs{h};
    h.openForWrite(eckit::Length(0));
    {
        eckit::AutoClose closer(h);
        /// @note: in the POSIX back-end, keeping a timestamp per index is necessary, to allow
        ///   determining which was the latest indexed field in cases where multiple processes
        ///   index a same field or in cases where multiple catalogues are combined with DistFDB.
        ///   In the DAOS back-end, however, determining the latest indexed field is straigthforward
        ///   as all parallel processes writing fields for a same index key will share a DAOS
        ///   key-value, and the last indexing will supersede the previous ones.
        ///   DistFDB will be obsoleted in favour of a centralised catalogue mechanism which can
        ///   index fields on multiple catalogues.
        ///   Therefore keeping timestamps in DAOS should not be necessary.
        ///   They are kept for now only for informational purposes.
        takeTimestamp();
        hs << timestamp();
        hs << field.location();
    }

    int field_loc_max_len = 512;  /// @todo: read from config
    if (hs.bytesWritten() > field_loc_max_len)
        throw eckit::Exception("Serialised field location exceeded configured maximum location length.");

    fdb5::DaosSession s{};

    /// @note: performed RPCs:
    /// - ensure index kv exists (daos_obj_open)
    /// - record field key and location into index kv (daos_kv_put)
    /// - close index kv when destroyed (daos_obj_close)
    fdb5::DaosKeyValue{s, location_.daosName()}.put(key.valuesToString(), h.data(), hs.bytesWritten());
}

void DaosIndex::entries(EntryVisitor& visitor) const {

    Index instantIndex(const_cast<DaosIndex*>(this));

    // Allow the visitor to selectively decline to visit the entries in this index
    if (visitor.visitIndex(instantIndex)) {

        fdb5::DaosSession s{};

        /// @note: performed RPCs:
        /// - index kv open (daos_obj_open)
        /// - index kv list keys (daos_kv_list)
        fdb5::DaosKeyValue index_kv{s, location_.daosName()};

        for (const auto& key : index_kv.keys()) {

            if (key == "axes" || key == "key")
                continue;

            /// @note: the DaosCatalogue is currently indexing a serialised DaosFieldLocation for each
            ///   archived field key. In the list pathway, DaosLazyFieldLocations are built for all field
            ///   keys present in an index -- without retrieving the actual location --, and
            ///   ListVisitor::visitDatum is called for each (see note at the top of DaosLazyFieldLocation.h).
            ///   When a field key is matched in visitDatum, DaosLazyFieldLocation::stableLocation is called,
            ///   which in turn calls this method here and triggers retrieval and deserialisation of the
            ///   indexed DaosFieldLocation, and returns it. Since the deserialised instance is of a
            ///   polymorphic class, it needs to be reanimated.
            auto loc = std::make_shared<fdb5::DaosLazyFieldLocation>(location_.daosName(), key);
            fdb5::Field field(loc, time_t(), fdb5::FieldDetails());
            visitor.visitDatum(field, key);
        }
    }
}

std::vector<eckit::URI> DaosIndex::dataURIs() const {

    /// @note: if daos index + daos store, this will return a uri to a DAOS array for each indexed field
    /// @note: if daos index + posix store, this will return a vector of unique uris to all referenced posix files
    ///   in this index (one for each writer process that has written to the index)
    /// @note: in the case where we have a daos store, the current implementation of dataURIs is unnecessarily
    /// inefficient.
    ///   This method is only called during wipe, where the uris obtained from this method are processed to
    ///   obtain unique store container paths - will always result in just one container uri! Having a URI store for
    ///   each index in DAOS could make this process more efficient, but it would imply more KV operations and slow down
    ///   field writes.
    /// @note: in the case where we have a posix store there will be more than one unique store file paths. The current
    ///   implementation is still inefficient but preferred to maintaining a URI store in the DAOS catalogue

    fdb5::DaosSession s{};
    fdb5::DaosKeyValue index_kv{s, location_.daosName()};

    std::set<eckit::URI> res;

    for (const auto& key : index_kv.keys()) {

        if (key == "axes" || key == "key")
            continue;

        std::vector<char> data;
        eckit::MemoryStream ms = index_kv.getMemoryStream(data, key, "index kv");

        time_t ts;
        ms >> ts;

        std::unique_ptr<fdb5::FieldLocation> fl(eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms));
        res.insert(fl->uri());
    }

    return std::vector<eckit::URI>(res.begin(), res.end());
}

//-----------------------------------------------------------------------------

}  // namespace fdb5

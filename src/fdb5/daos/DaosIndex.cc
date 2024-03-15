/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <limits.h>  // for PATH_MAX

#include "eckit/io/MemoryHandle.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/serialisation/HandleStream.h"
#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosIndex.h"
#include "fdb5/daos/DaosLazyFieldLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosIndex::DaosIndex(const Key& key, const fdb5::DaosKeyValueName& name) :
    IndexBase(key, "daosKeyValue"),
    location_(name, 0) {}

bool DaosIndex::mayContain(const Key &key) const {

    /// @todo: in-memory axes_ are left empty in the read pathway. Alternatively, could
    ///        consider reading axes content from DAOS in one go upon DaosIndex creation,
    ///        and have mayContain check these in-memory axes. However some mechanism
    ///        would have to be implemented for readers to lock DAOS KVs storing axis 
    ///        information so that in-memory axes are consistent with DAOS axes.

    /// @todo: visit in-memory axes anyway in case a writer process is later reading data, or 
    ///   in case in-memory axes are updated by a reader process (see todo below)?

    std::string indexKey{key_.valuesToString()};

    for (Key::const_iterator i = key.begin(); i != key.end(); ++i) {

        const std::string &keyword = i->first;
        std::string value = key.canonicalValue(keyword);

        /// @todo: take oclass from config
        fdb5::DaosKeyValueOID oid{indexKey + std::string{"."} + keyword, OC_S1};
        fdb5::DaosKeyValueName axis{location_.daosName().poolName(), location_.daosName().contName(), oid};

        /// @todo: update in-memory axes here to speed up future reads in the same process
        if (!axis.exists()) return false;  /// @todo: should throw an exception if the axis is not present in the "axes" list in the index kv?
        if (!axis.has(value)) return false;

    }

    return true;

}

const IndexAxis& DaosIndex::updatedAxes() {

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

    fdb5::DaosSession s{};
    const fdb5::DaosKeyValueName& index_kv_name = location_.daosName();

    /// @note: performed RPCs:
    /// - ensure axis kv exists (daos_obj_open)
    fdb5::StatsTimer st{"retrieve xx0 index kv open", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
    fdb5::DaosKeyValue index_kv{s, index_kv_name};
    st.stop();

    int axis_names_max_len = 512;  /// @todo: take from config
    std::vector<char> axes_data((long) axis_names_max_len);

    /// @note: performed RPCs:
    /// - get axes key size and content (daos_kv_get without buffer + daos_kv_get)
    st.start("retrieve xx1 index kv get axes", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
    long res = index_kv.get("axes", &axes_data[0], axis_names_max_len);
    st.stop();

    std::vector<std::string> axis_names;
    eckit::Tokenizer parse(",");
    parse(std::string(axes_data.begin(), std::next(axes_data.begin(), res)), axis_names);
    std::string indexKey{key_.valuesToString()};
    for (const auto& name : axis_names) {
        /// @todo: take oclass from config
        fdb5::DaosKeyValueOID oid(indexKey + std::string{"."} + name, OC_S1);
        fdb5::DaosKeyValueName nkv(index_kv_name.poolName(), index_kv_name.contName(), oid);

        /// @note: performed RPCs:
        /// - generate axis kv oid (daos_obj_generate_oid)
        /// - ensure axis kv exists (daos_obj_open)
        st.start("retrieve xx2 axis kv open", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
        fdb5::DaosKeyValue axis_kv{s, nkv};
        st.stop();

        /// @note: performed RPCs:
        /// - one or more kv list (daos_kv_list)
        st.start("retrieve xx3 axis kv list(s)", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
        axes_.insert(name, axis_kv.keys());
        st.stop();
    }

    return IndexBase::axes();

}

bool DaosIndex::get(const Key &key, const Key &remapKey, Field &field) const {

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

    const fdb5::DaosKeyValueName& n = location_.daosName();

    fdb5::DaosSession s{};

    /// @note: performed RPCs:
    /// - ensure index kv exists (daos_obj_open)
    fdb5::StatsTimer st{"retrieve 05 index kv open", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
    fdb5::DaosKeyValue index{s, n};
    st.stop();

    std::string query{key.valuesToString()};

    int field_loc_max_len = 512;  /// @todo: read from config
    std::vector<char> loc_data((long) field_loc_max_len);
    long res;

    try {

        /// @note: performed RPCs:
        /// - retrieve field array location from index kv (daos_kv_get)
        st.start("retrieve 06 index kv get field location", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
        res = index.get(query, &loc_data[0], field_loc_max_len);
        st.stop();

    } catch (fdb5::DaosEntityNotFoundException& e) {

        st.stop();

        /// @note: performed RPCs:
        /// - close index kv (daos_obj_close)
        st.start("retrieve 07 index kv close", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));

        return false;

    }

    eckit::MemoryStream ms{&loc_data[0], (size_t) res};

    /// @note: timestamp read for informational purpoes. See note in DaosIndex::add.
    time_t ts;
    ms >> ts;

    fdb5::FieldLocation* loc = eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms);
    field = fdb5::Field(std::move(*loc), ts, fdb5::FieldDetails());

    /// @note: performed RPCs:
    /// - close index kv (daos_obj_close)
    st.start("retrieve 07 index kv close", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));

    return true;

}

void DaosIndex::add(const Key &key, const Field &field) {

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();

    eckit::MemoryHandle h{(size_t) PATH_MAX};
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
    fdb5::StatsTimer st{"archive 12 index kv put field location", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
    fdb5::DaosKeyValue{s, location_.daosName()}.put(key.valuesToString(), h.data(), hs.bytesWritten());   

}

void DaosIndex::entries(EntryVisitor &visitor) const {

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::DaosManager::instance().timer();
    fdb5::DaosIOStats& stats = fdb5::DaosManager::instance().stats();
    
    Index instantIndex(const_cast<DaosIndex*>(this));

    // Allow the visitor to selectively decline to visit the entries in this index
    if (visitor.visitIndex(instantIndex)) {

        fdb5::DaosSession s{};

        /// @note: performed RPCs:
        /// - index kv open (daos_obj_open)
        /// - index kv list keys (daos_kv_list)
        fdb5::StatsTimer st{"list 010 index kv open and list keys", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
        fdb5::DaosKeyValue index_kv{s, location_.daosName()};

        for (const auto& key : index_kv.keys()) {
            st.stop();

            if (key == "axes" || key == "key") continue;

            fdb5::FieldLocation* loc = new fdb5::DaosLazyFieldLocation(location_.daosName(), key);
            fdb5::Field field(std::move(*loc), time_t(), fdb5::FieldDetails());
            visitor.visitDatum(field, key);

        }
        st.stop();

    }
}

const std::vector<eckit::URI> DaosIndex::dataURIs() const {

    /// @note: if daos index + daos store, this will return a uri to a DAOS array for each indexed field
    /// @note: if daos index + posix store, this will return a vector of unique uris to all referenced posix files
    ///   in this index (one for each writer process that has written to the index)
    /// @note: in the case where we have a daos store, the current implementation of dataURIs is unnecessarily inefficient.
    ///   This method is only called in DaosWipeVisitor, where the uris obtained from this method are processed to obtain
    ///   unique store container paths - will always result in just one container uri! Having a URI store for each index in
    ///   DAOS could make this process more efficient, but it would imply more KV operations and slow down field writes.
    /// @note: in the case where we have a posix store there will be more than one unique store file paths. The current 
    ///   implementation is still inefficient but preferred to maintaining a URI store in the DAOS catalogue

    fdb5::DaosSession s{};
    fdb5::DaosKeyValue index_kv{s, location_.daosName()};

    std::set<eckit::URI> res;

    for (const auto& key : index_kv.keys()) {

        if (key == "axes" || key == "key") continue;

        daos_size_t size{index_kv.size(key)};
        std::vector<char> v((long) size);
        index_kv.get(key, &v[0], size);
        eckit::MemoryStream ms{&v[0], size};
        
        time_t ts;
        ms >> ts;

        std::unique_ptr<fdb5::FieldLocation> fl(eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms));
        res.insert(fl->uri());

    }

    return std::vector<eckit::URI>(res.begin(), res.end());

}

//-----------------------------------------------------------------------------

} // namespace fdb5

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
#include "fdb5/daos/DaosFieldLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosIndex::DaosIndex(const Key& key, const fdb5::DaosKeyValueName& name) :
    IndexBase(key, "daosKeyValue"),
    location_(name, 0) {
}

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

    fdb5::DaosSession s{};
    const fdb5::DaosKeyValueName& index_kv_name = location_.daosName();
    fdb5::DaosKeyValue index_kv{s, index_kv_name};
    daos_size_t size = index_kv.size("axes");
    std::vector<char> axes_data((long) size);
    index_kv.get("axes", &axes_data[0], size);
    std::vector<std::string> axis_names;
    eckit::Tokenizer parse(",");
    parse(std::string(axes_data.begin(), axes_data.end()), axis_names);
    std::string indexKey{key_.valuesToString()};
    for (const auto& name : axis_names) {
        /// @todo: take oclass from config
        fdb5::DaosKeyValueOID oid(indexKey + std::string{"."} + name, OC_S1);
        fdb5::DaosKeyValueName nkv(index_kv_name.poolName(), index_kv_name.contName(), oid);
        fdb5::DaosKeyValue axis_kv{s, nkv};
        axes_.insert(name, axis_kv.keys());
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
    /// - open pool if not cached (daos_pool_connect) -- always skipped, always cached/open after selectDatabase
    /// - check if cont exists if not cached (daos_cont_open) -- always skipped, always cached/open after selectDatabase
    /// - open container if not open (daos_cont_open) -- always skipped, always cached/open after selectDatabase
    /// - ensure index kv exists (daos_obj_open) -- always performed, objects not cached for now
    fdb5::StatsTimer st{"retrieve 05 index kv open", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
    fdb5::DaosKeyValue index{s, n};
    st.stop();

    std::string query{key.valuesToString()};

    /// @note: performed RPCs:
    /// - check if index kv contains index key (daos_kv_get without a buffer) -- always performed
    st.start("retrieve 06 index kv check", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
    if (!index.has(query)) {
        st.stop();

        /// @note: performed RPCs:
        /// - close index kv (daos_obj_close) -- always performed
        st.start("retrieve 08 index kv close", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));

        return false;
    }
    st.stop();

    /// @note: performed RPCs:
    /// - retrieve field array location from index kv (daos_kv_get without a buffer + daos_kv_get) -- always performed
    st.start("retrieve 07 index kv get field location", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));
    std::vector<char> loc_data((long) index.size(query));
    index.get(query, &loc_data[0], loc_data.size());
    st.stop();

    eckit::MemoryStream ms{&loc_data[0], loc_data.size()};

    /// @note: timestamp read for informational purpoes. See note in DaosIndex::add.
    time_t ts;
    ms >> ts;

    field = fdb5::Field(eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms), ts, fdb5::FieldDetails());

    /// @note: performed RPCs:
    /// - close index kv (daos_obj_close) -- always performed
    st.start("retrieve 08 index kv close", std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2));

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
    fdb5::DaosSession s{};

    /// @note: performed RPCs:
    /// - open pool if not cached (daos_pool_connect) -- always skipped, always cached/open after selectDatabase
    /// - check if cont exists if not cached (daos_cont_open) -- always skipped, always cached/open after selectDatabase
    /// - open container if not open (daos_cont_open) -- always skipped, always cached/open after selectDatabase
    /// - ensure index kv exists (daos_obj_open) -- always performed, objects not cached for now. Should be cached
    /// - record field key and location into index kv (daos_kv_put) -- always performed
    /// - close index kv when destroyed (daos_obj_close) -- always performed
    fdb5::StatsTimer st{"archive 12 index kv put field location", timer, std::bind(&fdb5::DaosIOStats::logMdOperation, &stats, _1, _2)};
    fdb5::DaosKeyValue{s, location_.daosName()}.put(key.valuesToString(), h.data(), hs.bytesWritten());   

}

void DaosIndex::entries(EntryVisitor &visitor) const {

    Index instantIndex(const_cast<DaosIndex*>(this));

    // Allow the visitor to selectively decline to visit the entries in this index
    if (visitor.visitIndex(instantIndex)) {

        fdb5::DaosSession s{};
        fdb5::DaosKeyValue index_kv{s, location_.daosName()};

        for (const auto& key : index_kv.keys()) {

            if (key == "axes" || key == "key") continue;

            daos_size_t size{index_kv.size(key)};
            std::vector<char> v((long) size);
            index_kv.get(key, &v[0], size);
            eckit::MemoryStream ms{&v[0], size};

            /// @note: timestamp read for informational purpoes. See note in DaosIndex::add.
            time_t ts;
            ms >> ts;

            std::unique_ptr<fdb5::FieldLocation> fl(eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms));
            fdb5::Field field(fl.get(), ts, fdb5::FieldDetails());
            visitor.visitDatum(field, key);

        }

    }
}

const std::vector<eckit::URI> DaosIndex::dataPaths() const {

    /// @note: if daos index + daos store, this will return a uri to a DAOS array for each indexed field
    /// @note: if daos index + posix store, this will return a vector of unique uris to all referenced posix files
    ///   in this index (one for each writer process that has written to the index)
    /// @note: in the case where we have a daos store, the current implementation of dataPaths is unnecessarily inefficient.
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

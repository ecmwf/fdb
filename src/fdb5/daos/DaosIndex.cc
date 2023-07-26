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

bool DaosIndex::get(const Key &key, const Key &remapKey, Field &field) const {

    const fdb5::DaosKeyValueName& n = location_.daosName();
    ASSERT(n.exists());

    std::string query{key.valuesToString()};

    if (!n.has(query)) return false;

    fdb5::DaosSession s{};
    fdb5::DaosKeyValue index{s, n};
    std::vector<char> loc_data((long) index.size(query));
    index.get(query, &loc_data[0], loc_data.size());
    eckit::MemoryStream ms{&loc_data[0], loc_data.size()};
    field = fdb5::Field(eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms), timestamp_, fdb5::FieldDetails());

    return true;

}

void DaosIndex::add(const Key &key, const Field &field) {

    eckit::MemoryHandle h{(size_t) PATH_MAX};
    eckit::HandleStream hs{h};
    h.openForWrite(eckit::Length(0));
    {
        eckit::AutoClose closer(h);
        hs << field.location();
    }
    fdb5::DaosSession s{};
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
            std::unique_ptr<fdb5::FieldLocation> fl(eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms));

            fdb5::Field field(new DaosFieldLocation(fl->uri()), visitor.indexTimestamp(), fdb5::FieldDetails());
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
        std::unique_ptr<fdb5::FieldLocation> fl(eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms));
        res.insert(fl->uri());

    }

    return std::vector<eckit::URI>(res.begin(), res.end());

}

//-----------------------------------------------------------------------------

} // namespace fdb5

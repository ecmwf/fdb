/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/rados/RadosIndex.h"

#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldDetails.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/rados/RadosLazyFieldLocation.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/Length.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/rados/RadosException.h"
#include "eckit/io/rados/RadosKeyValue.h"
#include "eckit/io/rados/RadosNamespace.h"
#include "eckit/serialisation/HandleStream.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/serialisation/Reanimator.h"
#include "eckit/utils/Tokenizer.h"

#include <climits>  // for PATH_MAX
#include <cstddef>
#include <ctime>
#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RadosIndex::RadosIndex(const Key& key, const eckit::RadosNamespace& name) :
    IndexBase(key, "radosKeyValue"),
    location_(eckit::RadosKeyValue{name.pool().name(), name.name(), key.valuesToString()}, 0),
    idx_kv_(location_.radosName().uri()) {

    /// @note: performed RPCs:
    /// - generate index kv oid (daos_obj_generate_oid)
    /// - create/open index kv (daos_kv_open)

    /// write indexKey under "key"
    eckit::MemoryHandle h{(size_t)PATH_MAX};
    eckit::HandleStream hs{h};
    h.openForWrite(eckit::Length(0));
    {
        eckit::AutoClose closer(h);
        hs << key;
    }

    /// @note: performed RPCs:
    /// - record index key into index kv (daos_kv_put)
    idx_kv_.put("key", h.data(), hs.bytesWritten());
}

RadosIndex::RadosIndex(const Key& key, const eckit::RadosKeyValue& name, bool readAxes) :
    IndexBase(key, "radosKeyValue"), location_(name, 0), idx_kv_(name.uri()) {

    if (readAxes) {
        updateAxes();
    }
}

void RadosIndex::putAxisNames(const std::string& names) {

    idx_kv_.put("axes", names.data(), names.length());
}

void RadosIndex::putAxisValue(const std::string& axis, const std::string& value) {

    auto axis_kv = axis_kvs_.find(axis);

    if (axis_kv == axis_kvs_.end()) {
        std::string kv_name = key().valuesToString() + std::string{"."} + axis;
        axis_kvs_.emplace(std::piecewise_construct, std::forward_as_tuple(axis),
                          std::forward_as_tuple(location_.radosName().nspace().pool().name(),
                                                location_.radosName().nspace().name(), kv_name));

        axis_kv = axis_kvs_.find(axis);
    }

    std::string v{"1"};
    axis_kv->second.put(value, v.data(), v.length());
}

void RadosIndex::updateAxes() {

    /// @note: performed RPCs:
    /// - ensure axis kv exists (daos_obj_open)

    /// @note: performed RPCs:
    /// - get axes key size and content (daos_kv_get without buffer + daos_kv_get)
    std::vector<char> axes_data;
    idx_kv_.getMemoryStream(axes_data, "axes", "index kv");

    std::vector<std::string> axis_names;
    eckit::Tokenizer parse(",");
    parse(std::string(axes_data.begin(), axes_data.end()), axis_names);
    std::string indexKey{key_.valuesToString()};
    for (const auto& name : axis_names) {
        /// @note: performed RPCs:
        /// - generate axis kv oid (daos_obj_generate_oid)
        /// - ensure axis kv exists (daos_obj_open)
        eckit::RadosKeyValue axis_kv{idx_kv_.nspace().pool().name(), idx_kv_.nspace().name(),
                                     indexKey + std::string{"."} + name};

        /// @note: performed RPCs:
        /// - one or more kv list (daos_kv_list)
        axes_.insert(name, axis_kv.keys());
    }

    axes_.sort();
}

bool RadosIndex::get(const Key& key, const Key& remapKey, Field& field) const {

    /// @note: performed RPCs:
    /// - ensure index kv exists (daos_obj_open)

    std::string query{key.valuesToString()};

    try {

        /// @note: performed RPCs:
        /// - retrieve field array location from index kv (daos_kv_get)
        std::vector<char> loc_data;
        eckit::MemoryStream ms = idx_kv_.getMemoryStream(loc_data, query, "index kv");

        /// @note: timestamp read for informational purpoes. See note in DaosIndex::add.
        time_t ts;
        ms >> ts;

        fdb5::FieldLocation* loc = eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms);
        field = fdb5::Field(std::move(*loc), ts, fdb5::FieldDetails());
    }
    catch (eckit::RadosEntityNotFoundException& e) {

        /// @note: performed RPCs:
        /// - close index kv (daos_obj_close)

        return false;
    }

    /// @note: performed RPCs:
    /// - close index kv (daos_obj_close)

    return true;
}

void RadosIndex::add(const Key& key, const Field& field) {

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

    /// @note: performed RPCs:
    /// - ensure index kv exists (daos_obj_open)
    /// - record field key and location into index kv (daos_kv_put)
    /// - close index kv when destroyed (daos_obj_close)
    idx_kv_.put(key.valuesToString(), h.data(), hs.bytesWritten());
}

void RadosIndex::entries(EntryVisitor& visitor) const {

    Index instantIndex(const_cast<RadosIndex*>(this));

    // Allow the visitor to selectively decline to visit the entries in this index
    if (visitor.visitIndex(instantIndex)) {

        /// @note: performed RPCs:
        /// - index kv open (daos_obj_open)
        /// - index kv list keys (daos_kv_list)

        for (const auto& key : idx_kv_.keys()) {

            if (key == "axes" || key == "key") {
                continue;
            }

            /// @note: the DaosCatalogue is currently indexing a serialised DaosFieldLocation for each
            ///   archived field key. In the list pathway, DaosLazyFieldLocations are built for all field
            ///   keys present in an index -- without retrieving the actual location --, and
            ///   ListVisitor::visitDatum is called for each (see note at the top of DaosLazyFieldLocation.h).
            ///   When a field key is matched in visitDatum, DaosLazyFieldLocation::stableLocation is called,
            ///   which in turn calls this method here and triggers retrieval and deserialisation of the
            ///   indexed DaosFieldLocation, and returns it. Since the deserialised instance is of a
            ///   polymorphic class, it needs to be reanimated.
            fdb5::FieldLocation* loc = new fdb5::RadosLazyFieldLocation(location_.radosName(), key);
            fdb5::Field field(std::move(*loc), time_t(), fdb5::FieldDetails());
            visitor.visitDatum(field, key);
        }
    }
}

std::vector<eckit::URI> RadosIndex::dataURIs() const {

    /// @note: if daos index + daos store, this will return a uri to a DAOS array for each indexed field
    /// @note: if daos index + posix store, this will return a vector of unique uris to all referenced posix files
    ///   in this index (one for each writer process that has written to the index)
    /// @note: in the case where we have a daos store, the current implementation of dataURIs is unnecessarily
    /// inefficient.
    ///   This method is only called in DaosWipeVisitor, where the uris obtained from this method are processed to
    ///   obtain unique store container paths - will always result in just one container uri! Having a URI store for
    ///   each index in DAOS could make this process more efficient, but it would imply more KV operations and slow down
    ///   field writes.
    /// @note: in the case where we have a posix store there will be more than one unique store file paths. The current
    ///   implementation is still inefficient but preferred to maintaining a URI store in the DAOS catalogue

    std::set<eckit::URI> res;

    for (const auto& key : idx_kv_.keys()) {

        if (key == "axes" || key == "key") {
            continue;
        }

        std::vector<char> data;
        eckit::MemoryStream ms = idx_kv_.getMemoryStream(data, key, "index kv");

        time_t ts;
        ms >> ts;

        std::unique_ptr<fdb5::FieldLocation> fl(eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms));
        res.insert(fl->uri());
    }

    return std::vector<eckit::URI>(res.begin(), res.end());
}

//-----------------------------------------------------------------------------

}  // namespace fdb5

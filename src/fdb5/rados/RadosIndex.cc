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

    // Persist indexKey under "key" so the index KV can later be identified when reopened.
    eckit::MemoryHandle h{(size_t)PATH_MAX};
    eckit::HandleStream hs{h};
    h.openForWrite(eckit::Length(0));
    {
        eckit::AutoClose closer(h);
        hs << key;
    }

    idx_kv_.put("key", h.data(), hs.bytesWritten());
}

RadosIndex::RadosIndex(const Key& key, const eckit::RadosKeyValue& name, bool readAxes) :
    IndexBase(key, "radosKeyValue"), location_(name, 0), idx_kv_(name.uri()) {

    if (readAxes) {
        updateAxes();
    }
}

void RadosIndex::putAxisValue(const std::string& axis, const std::string& value) {

    const std::string axis_marker = "axis." + axis;
    const char marker = '1';
    idx_kv_.put(axis_marker, &marker, 1);

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

    std::set<std::string> axis_names;
    for (const auto& key : idx_kv_.keys()) {
        if (key.rfind("axis.", 0) == 0) {
            axis_names.insert(key.substr(5));
        }
    }

    // Compatibility with catalogues written before per-axis markers were introduced.
    if (axis_names.empty() && idx_kv_.has("axes")) {
        std::vector<char> axes_data;
        idx_kv_.getMemoryStream(axes_data, "axes", "index kv");
        std::vector<std::string> legacy_axis_names;
        eckit::Tokenizer parse(",");
        parse(std::string(axes_data.begin(), axes_data.end()), legacy_axis_names);
        axis_names.insert(legacy_axis_names.begin(), legacy_axis_names.end());
    }

    std::string indexKey{key_.valuesToString()};
    for (const auto& name : axis_names) {
        eckit::RadosKeyValue axis_kv{idx_kv_.nspace().pool().name(), idx_kv_.nspace().name(),
                                     indexKey + std::string{"."} + name};

        axes_.insert(name, axis_kv.keys());
    }

    axes_.sort();
}

bool RadosIndex::get(const Key& key, const Key& remapKey, Field& field) const {

    std::string query{key.valuesToString()};

    try {
        std::vector<char> loc_data;
        eckit::MemoryStream ms = idx_kv_.getMemoryStream(loc_data, query, "index kv");

        // Timestamp is read for informational purposes only; see note in RadosIndex::add.
        time_t ts;
        ms >> ts;

        fdb5::FieldLocation* loc = eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms);
        field = fdb5::Field(std::move(*loc), ts, fdb5::FieldDetails());
    }
    catch (eckit::RadosEntityNotFoundException& e) {
        return false;
    }

    return true;
}

void RadosIndex::add(const Key& key, const Field& field) {

    eckit::MemoryHandle h{(size_t)PATH_MAX};
    eckit::HandleStream hs{h};
    h.openForWrite(eckit::Length(0));
    {
        eckit::AutoClose closer(h);
        // Timestamp kept per-entry for informational purposes; correctness does not depend on it because
        // parallel writers targeting the same index key share this KV and last-write-wins.
        takeTimestamp();
        hs << timestamp();
        hs << field.location();
    }

    idx_kv_.put(key.valuesToString(), h.data(), hs.bytesWritten());
}

void RadosIndex::entries(EntryVisitor& visitor) const {

    Index instantIndex(const_cast<RadosIndex*>(this));

    // Allow the visitor to selectively decline to visit the entries in this index
    if (visitor.visitIndex(instantIndex)) {

        for (const auto& key : idx_kv_.keys()) {

            if (key == "axes" || key == "key" || key.rfind("axis.", 0) == 0) {
                continue;
            }

            // Build a lazy location so ListVisitor::visitDatum can filter without triggering a KV read.
            // The real FieldLocation is retrieved and reanimated only when stableLocation() is called.
            auto loc = std::make_shared<fdb5::RadosLazyFieldLocation>(location_.radosName(), key);
            fdb5::Field field(loc, time_t(), fdb5::FieldDetails());
            visitor.visitDatum(field, key);
        }
    }
}

std::vector<eckit::URI> RadosIndex::dataURIs() const {

    // Iterates the index KV; each entry is a serialised RadosFieldLocation.

    std::set<eckit::URI> res;

    for (const auto& key : idx_kv_.keys()) {

        if (key == "axes" || key == "key" || key.rfind("axis.", 0) == 0) {
            continue;
        }

        std::vector<char> data;
        eckit::MemoryStream ms = idx_kv_.getMemoryStream(data, key, "index kv");

        time_t ts;
        ms >> ts;

        std::unique_ptr<fdb5::FieldLocation> fl(eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms));
        res.insert(fl->uri());
    }

    return {res.begin(), res.end()};
}

//-----------------------------------------------------------------------------

}  // namespace fdb5

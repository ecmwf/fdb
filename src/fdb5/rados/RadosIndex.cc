// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/rados/RadosIndex.h"

#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldDetails.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/rados/RadosCommon.h"
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
    location_(eckit::RadosKeyValue{name.pool().name(), name.name(), rados_names::indexPrefix + key.valuesToString()},
              0),
    idx_kv_(location_.radosName().uri()),
    created_(false) {}

RadosIndex::RadosIndex(const Key& key, const eckit::RadosKeyValue& name, bool readAxes) :
    IndexBase(key, "radosKeyValue"), location_(name, 0), idx_kv_(name.uri()) {

    if (readAxes) {
        updateAxes();
    }
}

std::set<std::string> RadosIndex::axisNames(const eckit::RadosKeyValue& idx_kv) {

    const std::string prefix = rados_names::axisPrefix;

    std::set<std::string> axis_names;
    for (const auto& key : idx_kv.keys()) {
        if (rados_names::has_prefix(key, prefix)) {
            axis_names.insert(key.substr(prefix.size()));
        }
    }

    return axis_names;
}

eckit::RadosKeyValue RadosIndex::axisKeyValue(const eckit::RadosKeyValue& idx_kv, const Key& key,
                                              const std::string& axis) {
    return {idx_kv.nspace().pool().name(), idx_kv.nspace().name(),
            rados_names::axisPrefix + key.valuesToString() + "." + axis};
}

void RadosIndex::putAxisValue(const std::string& axis, const std::string& value) {
    pendingAxisValues_[axis].insert(value);
}

void RadosIndex::flush() {

    if (!created_) {
        // Persist the index key under "key" so the index KV can later be identified when reopened.
        const auto key = serialise([this](eckit::Stream& s) { s << key_; });
        idx_kv_.put("key", key.data(), key.size());
        created_ = true;
    }

    // Axis values go first: a reader must never find an entry whose axis values are not yet recorded.
    const char marker = '1';
    for (const auto& [axis, values] : pendingAxisValues_) {

        idx_kv_.put(rados_names::axisPrefix + axis, &marker, 1);

        auto axis_kv = axis_kvs_.find(axis);
        if (axis_kv == axis_kvs_.end()) {
            axis_kv = axis_kvs_.emplace(axis, axisKeyValue(idx_kv_, key(), axis)).first;
        }

        for (const auto& value : values) {
            axis_kv->second.put(value, &marker, 1);
        }
    }
    pendingAxisValues_.clear();

    for (const auto& [key, location] : pendingEntries_) {
        idx_kv_.put(key, location.data(), location.size());
    }
    pendingEntries_.clear();
}

void RadosIndex::updateAxes() {

    for (const auto& name : axisNames(idx_kv_)) {
        axes_.insert(name, axisKeyValue(idx_kv_, key_, name).keys());
    }

    axes_.sort();
}

bool RadosIndex::get(const Key& key, const Key& remapKey, Field& field) const {

    const std::string query = rados_names::fieldPrefix + key.valuesToString();

    try {
        Entry entry = readEntry(idx_kv_, query);
        field = fdb5::Field(std::move(entry.location), entry.timestamp, fdb5::FieldDetails());
    }
    catch (eckit::RadosEntityNotFoundException& e) {
        return false;
    }

    return true;
}

void RadosIndex::add(const Key& key, const Field& field) {

    // Timestamp kept per-entry for informational purposes; correctness does not depend on it because
    // parallel writers targeting the same index key share this KV and last-write-wins.
    takeTimestamp();

    pendingEntries_[rados_names::fieldPrefix + key.valuesToString()] = serialise([&](eckit::Stream& s) {
        s << timestamp();
        s << field.location();
    });
}

RadosIndex::Entry RadosIndex::readEntry(const eckit::RadosKeyValue& idx_kv, const std::string& name) {

    std::vector<char> data;
    eckit::MemoryStream ms = idx_kv.getMemoryStream(data, name, "index kv");

    Entry entry;
    ms >> entry.timestamp;
    entry.location.reset(eckit::Reanimator<fdb5::FieldLocation>::reanimate(ms));

    return entry;
}

void RadosIndex::entries(EntryVisitor& visitor, EntryVisitor::IndexScope& scope) const {

    const std::string prefix = rados_names::fieldPrefix;

    for (const auto& key : idx_kv_.keys()) {

        if (!rados_names::has_prefix(key, prefix)) {
            continue;
        }

        // Build a lazy location so ListVisitor::visitDatum can filter without triggering a KV read.
        // The real FieldLocation is retrieved and reanimated only when stableLocation() is called.
        auto loc = std::make_shared<fdb5::RadosLazyFieldLocation>(location_.radosName(), key);
        fdb5::Field field(loc, time_t(), fdb5::FieldDetails());
        visitor.visitDatum(scope, field, key.substr(prefix.size()));
    }
}

std::vector<eckit::URI> RadosIndex::dataURIs() const {

    // Iterates the index KV; each entry is a serialised RadosFieldLocation.

    std::set<eckit::URI> res;

    for (const auto& key : idx_kv_.keys()) {

        if (!rados_names::has_prefix(key, rados_names::fieldPrefix)) {
            continue;
        }

        res.insert(readEntry(idx_kv_, key).location->uri());
    }

    return {res.begin(), res.end()};
}

//-----------------------------------------------------------------------------

}  // namespace fdb5

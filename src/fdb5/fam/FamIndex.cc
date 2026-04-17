/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

#include "fdb5/fam/FamIndex.h"

#include <climits>
#include <utility>

#include "eckit/io/MemoryHandle.h"
#include "eckit/log/Log.h"
#include "eckit/serialisation/HandleStream.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/serialisation/Reanimator.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/fam/FamCommon.h"
#include "fdb5/fam/FamIndexLocation.h"

namespace fdb5 {

namespace {

//----------------------------------------------------------------------------------------------------------------------
// Helpers

template <typename T>
eckit::Buffer serialize(std::size_t initial_capacity, T&& writer) {
    eckit::MemoryHandle handle{initial_capacity};
    eckit::HandleStream stream{handle};
    handle.openForWrite(0);
    {
        eckit::AutoClose closer(handle);
        std::forward<T>(writer)(stream);
    }
    return {handle.data(), static_cast<std::size_t>(stream.bytesWritten())};
}

/// Encode a field entry in wire format: [timestamp] [FieldLocation] [Key].
eckit::Buffer encodeIndex(const Field& field, const Key& key) {
    return serialize(static_cast<std::size_t>(PATH_MAX), [&](eckit::HandleStream& stream) {
        stream << field.timestamp();
        stream << field.location();
        stream << key;
    });
}

/// Decode the wire-format prefix: [timestamp] [FieldLocation]
/// @note the caller may continue reading Key
std::pair<time_t, std::shared_ptr<const FieldLocation>> decodePrefix(eckit::MemoryStream& stream) {
    time_t timestamp{};
    stream >> timestamp;
    auto location = std::shared_ptr<const FieldLocation>(eckit::Reanimator<const FieldLocation>::reanimate(stream));
    return {timestamp, std::move(location)};
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

FamIndex::FamIndex(const Key& key, const eckit::FamRegionName& region_name, const std::string& name, bool read_axes) :
    IndexBase(key, FamCommon::type),
    location_{region_name.object(name + FamCommon::table_suffix).uri()},
    data_{name, region_name.lookup()} {
    if (read_axes) {
        updateAxes();
    }
}

//----------------------------------------------------------------------------------------------------------------------

void FamIndex::add(const Key& key, const Field& field) {
    data_.forceInsert(FamCommon::toString(key), encodeIndex(field, key));
}

bool FamIndex::get(const Key& key, const Key& /*remapKey*/, Field& field) const {

    LOG_DEBUG_LIB(LibFdb5) << "FamIndex::get key=" << key << std::endl;

    const Map::key_type map_key{FamCommon::toString(key)};
    auto iter = data_.find(map_key);
    if (iter == data_.end()) {
        return false;
    }

    auto entry = *iter;  // copies FamMapEntry by value (FAM data → local Buffer)
    eckit::MemoryStream ms{entry.value};

    // no need to deserialize Key
    auto [timestamp, location] = decodePrefix(ms);

    field = Field(std::move(location), timestamp);

    return true;
}

void FamIndex::entries(EntryVisitor& visitor) const {

    Index instant_index(const_cast<FamIndex*>(this));

    // Allow the visitor to decline visiting this index's entries.
    if (!visitor.visitIndex(instant_index)) {
        LOG_DEBUG_LIB(LibFdb5) << "FamIndex::entries visitor declined index=" << instant_index << std::endl;
        return;
    }

    for (const auto& [key, value] : data_) {

        // Skip the reserved axes metadata entry.
        if (key.asString() == FamCommon::axes_keyword) {
            continue;
        }

        eckit::MemoryStream stream{value};
        auto [timestamp, location] = decodePrefix(stream);
        auto datum = Key(stream).valuesToString();

        // Use the public visitDatum(field, keyFingerprint) overload; it calls rule_->makeKey()
        // to reconstruct the Key with keyword names before dispatching to the protected override.
        visitor.visitDatum(Field(std::move(location), timestamp), datum);
    }
}

void FamIndex::updateAxes() {

    // Fast path: read the persisted axes snapshot if available.
    const Map::key_type axes_key{FamCommon::axes_keyword};
    if (auto iter = data_.find(axes_key); iter != data_.end()) {
        auto axes = (*iter).value;
        eckit::MemoryStream stream{axes};
        axes_.decode(stream, IndexAxis::currentVersion());
        return;
    }

    // Slow path (legacy / first open): full-scan all data entries.
    LOG_DEBUG_LIB(LibFdb5) << "FamIndex::updateAxes: no persisted axes, falling back to full scan" << std::endl;

    for (const auto& [key, value] : data_) {
        // Skip the axes metadata entry
        if (key.asString() == FamCommon::axes_keyword) {
            continue;
        }
        eckit::MemoryStream stream{value};
        decodePrefix(stream);  // skip timestamp + FieldLocation
        axes_.insert(Key{stream});
    }

    axes_.sort();
}

//----------------------------------------------------------------------------------------------------------------------

void FamIndex::dump(std::ostream& out, const char* indent, bool simple, bool dump_fields) const {
    out << indent << "Key: " << key_;

    if (!simple) {
        out << '\n';
        axes_.dump(out, indent);
    }

    if (dump_fields) {
        out << '\n' << indent << "Contents of index:" << '\n';
        const std::string child_indent = std::string(indent) + "  ";
        for (const auto& [key, value] : data_) {
            if (key.asString() == FamCommon::axes_keyword) {
                continue;
            }
            eckit::MemoryStream stream{value};
            auto [timestamp, location] = decodePrefix(stream);
            out << child_indent << "Fingerprint: " << key.asString() << ", location: " << *location << '\n';
        }
    }
}
void FamIndex::encode(eckit::Stream& /*s*/, const int /*version*/) const {
    NOTIMP;
}
void FamIndex::visit(IndexLocationVisitor& /*visitor*/) const {
    NOTIMP;
}

void FamIndex::print(std::ostream& out) const {
    out << "FamIndex[key=" << key_ << ",location=" << location_ << "]";
}

void FamIndex::flock() const {
    NOTIMP;
}

void FamIndex::funlock() const {
    NOTIMP;
}

std::vector<eckit::URI> FamIndex::dataURIs() const {
    return {};
}

bool FamIndex::dirty() const {
    return axes_.dirty();
}

void FamIndex::open() {}

void FamIndex::reopen() {
    NOTIMP;
}

void FamIndex::close() {}

void FamIndex::flush() {
    // Keep the in-memory axes sorted so readers always see a consistent view.
    axes_.sort();

    // read-merge-write
    if (axes_.dirty()) {
        const Map::key_type axes_key{FamCommon::axes_keyword};

        // Read existing persisted axes and merge local axes into them.
        IndexAxis merged;
        if (auto iter = data_.find(axes_key); iter != data_.end()) {
            auto buf = (*iter).value;
            eckit::MemoryStream stream{buf};
            merged.decode(stream, IndexAxis::currentVersion());
        }
        merged.merge(axes_);
        merged.sort();

        auto payload = serialize(static_cast<std::size_t>(PATH_MAX), [&merged](eckit::HandleStream& stream) {
            merged.encode(stream, IndexAxis::currentVersion());
        });
        data_.insertOrAssign(axes_key, payload);
        axes_.clean();
    }
}

IndexStats FamIndex::statistics() const {
    NOTIMP;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

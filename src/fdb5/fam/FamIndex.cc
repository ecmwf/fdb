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

#include "eckit/io/MemoryHandle.h"
#include "eckit/serialisation/HandleStream.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/serialisation/Reanimator.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldDetails.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/fam/FamCommon.h"
#include "fdb5/fam/FamIndexLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FamIndex::FamIndex(const Key& key, const Catalogue& /*catalogue*/, const eckit::FamRegionName& region_name,
                   const std::string& data_map_name, bool read_axes) :
    IndexBase(key, FamCommon::type),
    location_(region_name.object(data_map_name + FamCommon::table_suffix).uri()),
    data_(data_map_name, region_name.lookup()) {
    if (read_axes) {
        updateAxes();
    }
}

//----------------------------------------------------------------------------------------------------------------------

void FamIndex::add(const Key& key, const Field& field) {

    // Wire format: [timestamp (time_t)] [FieldLocation (polymorphic)] [Key (with keyword names)]
    //
    // polymorphic FieldLocation serialisation (supported by eckit::Reanimator)
    // TODO(metin): implement a URI table (mapping uint64_t IDs → FAM object URIs) at the FamStore level
    // and FamFieldRefReducedWire, which will eliminate the Reanimator dependency
    eckit::MemoryHandle h{static_cast<size_t>(PATH_MAX)};
    eckit::HandleStream hs{h};
    h.openForWrite(0);
    {
        eckit::AutoClose closer(h);
        hs << field.timestamp();
        hs << field.location();
        hs << key;
    }

    data_.insert(FamCommon::toString(key), h.data(), static_cast<std::size_t>(hs.bytesWritten()));
}

bool FamIndex::get(const Key& key, const Key& /*remapKey*/, Field& field) const {

    LOG_DEBUG_LIB(LibFdb5) << "FamIndex::get key=" << key << std::endl;

    const Map::key_type map_key{FamCommon::toString(key)};
    auto it = data_.find(map_key);
    if (it == data_.end()) {
        return false;
    }

    auto entry                 = *it;  // copies FamMapEntry by value (FAM data → local Buffer)
    const eckit::Buffer& value = entry.value;
    eckit::MemoryStream ms{static_cast<const char*>(value.data()), value.size()};

    time_t ts{};
    ms >> ts;

    // Key follows FieldLocation in the wire format; we stop here — no need to deserialise it.
    auto loc = std::shared_ptr<FieldLocation>(eckit::Reanimator<FieldLocation>::reanimate(ms));

    field = Field(std::move(loc), ts, FieldDetails());
    return true;
}

void FamIndex::entries(EntryVisitor& visitor) const {

    Index instant_index(const_cast<FamIndex*>(this));

    // Allow the visitor to decline visiting this index's entries.
    if (!visitor.visitIndex(instant_index)) {
        return;
    }

    for (const auto& [k, value] : data_) {

        eckit::MemoryStream ms{value};

        time_t ts{};
        ms >> ts;

        auto loc = std::shared_ptr<const FieldLocation>(eckit::Reanimator<const FieldLocation>::reanimate(ms));

        Key datum_key(ms);

        Field field(std::move(loc), ts, FieldDetails());

        // Use the public visitDatum(field, keyFingerprint) overload; it calls rule_->makeKey()
        // to reconstruct the Key with keyword names before dispatching to the protected override.
        visitor.visitDatum(field, datum_key.valuesToString());
    }
}

void FamIndex::updateAxes() {

    for (const auto& [k, value] : data_) {

        eckit::MemoryStream ms{value};

        {
            // Discard the timestamp
            time_t ts{};
            ms >> ts;
            // Discard the FieldLocation
            std::unique_ptr<const FieldLocation>(eckit::Reanimator<const FieldLocation>::reanimate(ms));
        }

        axes_.insert(Key{ms});
    }

    axes_.sort();
}

//----------------------------------------------------------------------------------------------------------------------

void FamIndex::dump(std::ostream& /*out*/, const char* /*indent*/, bool /*simple*/, bool /*dump_fields*/) const {
    NOTIMP;
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

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

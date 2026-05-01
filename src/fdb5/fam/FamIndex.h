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

/// @file   FamIndex.h
/// @author Metin Cakircali
/// @date   Mar 2026

#pragma once

#include "eckit/io/fam/FamMap.h"
#include "eckit/io/fam/FamRegionName.h"

#include "fdb5/database/Index.h"
#include "fdb5/fam/FamIndexLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// FAM-backed index using FamMap<FamMapEntry<128>>.
///
/// Each entry in the data map is keyed by the datum key values string and stores
/// the serialised timestamp + FieldLocation + Key (with keyword names).
/// This allows both fast point lookups and full iteration for list/wipe operations.
class FamIndex : public IndexBase {

public:  // types

    using Map = eckit::FamMap128;

public:  // methods

    /// Construct (or reopen) an index backed by a FamMap in the given region.
    /// @param key           The index key (identifies this index within the catalogue).
    /// @param region_name   FAM region that hosts the map objects.
    /// @param name          Name prefix for the data map FAM objects (≤30 chars recommended).
    /// @param read_axes     If true, populate in-memory axes by scanning the map on open.
    FamIndex(const Key& key, const eckit::FamRegionName& region_name, const std::string& name, bool read_axes = false);

    void flock() const override;
    void funlock() const override;

    /// Scan all entries and populate in-memory IndexAxis.
    void updateAxes();

private:  // methods

    const IndexLocation& location() const override { return location_; }

    std::vector<eckit::URI> dataURIs() const override;

    bool dirty() const override;

    void open() override;
    void close() override;
    void reopen() override;

    void visit(IndexLocationVisitor& visitor) const override;

    bool get(const Key& key, const Key& remap_key, Field& field) const override;
    void add(const Key& key, const Field& field) override;
    void flush() override;
    void encode(eckit::Stream& s, int version) const override;
    void entries(EntryVisitor& visitor) const override;

    void print(std::ostream& out) const override;
    void dump(std::ostream& out, const char* indent, bool simple, bool dump_fields) const override;

    IndexStats statistics() const override;

private:  // members

    FamIndexLocation location_;
    Map data_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

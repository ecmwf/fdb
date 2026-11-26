// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Nicolau Manubens
/// @date Mar 2023

#pragma once

#include "fdb5/daos/DaosIndexLocation.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/database/Index.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


class DaosIndex : public IndexBase {

public:  // methods

    /// @note: creates a new index in DAOS, in the container pointed to by 'name'
    DaosIndex(const Key& key, const Catalogue& catalogue, const fdb5::DaosName& name);
    /// @note: used to represent and operate with an index which already exists in DAOS
    DaosIndex(const Key& key, const Catalogue& catalogue, const fdb5::DaosKeyValueName& name, bool readAxes = true);

    void flock() const override { NOTIMP; }
    void funlock() const override { NOTIMP; }

private:  // methods

    const IndexLocation& location() const override { return location_; }

    std::vector<eckit::URI> dataURIs() const override;

    bool dirty() const override { NOTIMP; }

    void open() override { NOTIMP; }
    void close() override {}
    void reopen() override { NOTIMP; }

    void visit(IndexLocationVisitor& visitor) const override { NOTIMP; }

    bool get(const Key& key, const Key& remapKey, Field& field) const override;
    void add(const Key& key, const Field& field) override;
    void flush() override { NOTIMP; }
    void encode(eckit::Stream& s, const int version) const override { NOTIMP; }
    void entries(EntryVisitor& visitor, EntryVisitor::IndexScope& scope) const override;

    void print(std::ostream& out) const override { NOTIMP; }
    void dump(std::ostream& out, const char* indent, bool simple = false, bool dumpFields = false) const override {
        NOTIMP;
    }

    IndexStats statistics() const override { NOTIMP; }

    /// @note: reads complete axis info from DAOS.
    void updateAxes();

private:  // members

    fdb5::DaosIndexLocation location_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

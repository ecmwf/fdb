// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Nicolau Manubens
/// @author Metin Cakircali
/// @date Jun 2024

#pragma once

#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/IndexStats.h"
#include "fdb5/database/Key.h"
#include "fdb5/rados/RadosIndexLocation.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/rados/RadosKeyValue.h"
#include "eckit/io/rados/RadosNamespace.h"

#include <ctime>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


class RadosIndex : public IndexBase {

public:  // methods

    // Describes a new index KV under `name`. Nothing is written to RADOS before the first flush().
    RadosIndex(const Key& key, const eckit::RadosNamespace& name);
    // Wraps an already-existing index KV.
    RadosIndex(const Key& key, const eckit::RadosKeyValue& name, bool readAxes = true);

    void flock() const override { NOTIMP; }
    void funlock() const override { NOTIMP; }

    // Queues an axis value; it is persisted into idx_kv_ / axis_kvs_ by flush().
    void putAxisValue(const std::string& axis, const std::string& value);

    // An entry of an index KV: the location of a field and the time it was archived.
    struct Entry {
        time_t timestamp{0};
        std::shared_ptr<const FieldLocation> location;
    };

    // Reads the entry stored under `name` (including its prefix). Throws
    // eckit::NotFound if it is missing.
    static Entry readEntry(const eckit::RadosKeyValue& idx_kv, const std::string& name);

    // Names of the axes recorded in an index KV; none if the KV is missing.
    static std::set<std::string> axisNames(const eckit::RadosKeyValue& idx_kv);
    // The KV holding the values of `axis` for the index stored in `idx_kv`.
    static eckit::RadosKeyValue axisKeyValue(const eckit::RadosKeyValue& idx_kv, const Key& key,
                                             const std::string& axis);

    // Exposed so RadosCatalogueReader can enumerate field entries directly for stats.
    const eckit::RadosKeyValue& idx_kv() const { return idx_kv_; }

private:  // methods

    const IndexLocation& location() const override { return location_; }
    std::vector<eckit::URI> dataURIs() const override;

    bool dirty() const override { return !pendingEntries_.empty() || !pendingAxisValues_.empty(); }

    void open() override { NOTIMP; };
    // The RADOS KV index holds no open file/handle state, so closing is a no-op.
    // Must not throw: invoked during normal read/list flows via eckit::AutoCloser.
    void close() override {}
    void reopen() override { NOTIMP; }

    void visit(IndexLocationVisitor& visitor) const override { NOTIMP; }

    bool get(const Key& key, const Key& remapKey, Field& field) const override;
    void add(const Key& key, const Field& field) override;
    // Persists the queued axis values and then the queued entries. Nothing is visible to readers before
    // this is called, so entries never refer to data the store has not flushed yet.
    void flush() override;
    void encode(eckit::Stream& s, const int version) const override { NOTIMP; }
    void entries(EntryVisitor& visitor, EntryVisitor::IndexScope& scope) const override;

    void print(std::ostream& out) const override { NOTIMP; }
    void dump(std::ostream& out, const char* indent, bool simple = false, bool dumpFields = false) const override {
        NOTIMP;
    }

    IndexStats statistics() const override { NOTIMP; }

    // Rehydrates the complete axis info from RADOS.
    void updateAxes();

private:  // members

    fdb5::RadosIndexLocation location_;

    eckit::RadosKeyValue idx_kv_;
    std::map<std::string, eckit::RadosKeyValue> axis_kvs_;

    // False for a new index until its KV has been written by flush().
    bool created_{true};

    // Serialised entries and axis values archived since the last flush().
    std::map<std::string, std::vector<char>> pendingEntries_;
    std::map<std::string, std::set<std::string>> pendingAxisValues_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

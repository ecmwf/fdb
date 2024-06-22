/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Nicolau Manubens
/// @date Jun 2024

#pragma once

#include "eckit/io/rados/RadosNamespace.h"
#include "eckit/io/rados/RadosKeyValue.h"
#include "eckit/io/rados/RadosPersistentKeyValue.h"

#include "fdb5/database/Index.h"
#include "fdb5/rados/RadosIndexLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


class RadosIndex : public IndexBase {

public: // methods

    /// @note: creates a new index in DAOS, in the container pointed to by 'name'
    RadosIndex(const Key& key, const eckit::RadosNamespace& name);
    /// @note: used to represent and operate with an index which already exists in DAOS
// #if defined(fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_WRITE) || defined(fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH)
//     RadosIndex(const Key& key, const eckit::RadosPersistentKeyValue& name, bool readAxes = true);
// #else
    RadosIndex(const Key& key, const eckit::RadosKeyValue& name, bool readAxes = true);
// #endif

    void flock() const override { NOTIMP; }
    void funlock() const override { NOTIMP; }

    /// @note: these methods are required for RadosCatalogueWriter to directly manipulate 
    /// idx_kv_ and axis_kvs_ within the RadosIndex. Upon flush, the index will flush all
    /// operations performed on these kvs (if PERSIST_ON_FLUSH).
    void putAxisNames(const std::string& names);
    void putAxisValue(const std::string& axis, const std::string& value);

private: // methods

    const IndexLocation& location() const override { return location_; }
    const std::vector<eckit::URI> dataURIs() const override;

    bool dirty() const override { NOTIMP; }

    void open() override { NOTIMP; };
    void close() override { NOTIMP; }
    void reopen() override { NOTIMP; }

    void visit(IndexLocationVisitor& visitor) const override { NOTIMP; }

    bool get( const Key &key, const Key &remapKey, Field &field ) const override;
    void add( const Key &key, const Field &field ) override;
#ifdef fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH
    void flush() override;
#else
    void flush() override { NOTIMP; }
#endif
    void encode(eckit::Stream& s, const int version) const override { NOTIMP; }
    void entries(EntryVisitor& visitor) const override;

    void print( std::ostream &out ) const override { NOTIMP; }
    void dump(std::ostream& out, const char* indent, bool simple = false, bool dumpFields = false) const override { NOTIMP; }

    IndexStats statistics() const override { NOTIMP; }

    /// @note: reads complete axis info from DAOS.
    void updateAxes();

private: // members

    fdb5::RadosIndexLocation location_;

#if defined(fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_WRITE) || defined(fdb5_HAVE_RADOS_BACKENDS_PERSIST_ON_FLUSH)
    eckit::RadosPersistentKeyValue idx_kv_;
    std::map<std::string, eckit::RadosPersistetKeyValue> axis_kvs_;
#else
    eckit::RadosKeyValue idx_kv_;
    std::map<std::string, eckit::RadosKeyValue> axis_kvs_;
#endif

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

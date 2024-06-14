/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Emanuele Danovaro
/// @author Nicolau Manubens
/// @date   Feb 2024

#pragma once

#include "eckit/io/rados/RadosObject.h"

#include "fdb5/fdb5_config.h"

#include "fdb5/database/Store.h"
#include "fdb5/rules/Schema.h"

#include "fdb5/rados/RadosCommon.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// Store that implements the FDB on CEPH object store

class RadosStore : public Store, public RadosCommon {

public: // methods

    RadosStore(const Schema& schema, const Key& key, const Config& config);

    ~RadosStore() override {}

    eckit::URI uri() const override;
    bool uriBelongs(const eckit::URI&) const override;
    bool uriExists(const eckit::URI&) const override;
    std::vector<eckit::URI> storeUnitURIs() const override;
    std::set<eckit::URI> asStoreUnitURIs(const std::vector<eckit::URI>&) const override;

    bool open() override { return true; }
    void flush() override;
    void close() override;

    void checkUID() const override { /* nothing to do */ }

protected: // methods

    std::string type() const override { return "rados"; }

    bool exists() const override;

    eckit::DataHandle* retrieve(Field& field) const override;
    std::unique_ptr<FieldLocation> archive(const Key& key, const void * data, eckit::Length length) override;

    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    void print(std::ostream &out) const override;

    void parseConfig(const fdb5::Config& config);
    
    eckit::RadosObject generateDataObject(const Key& key) const;

#ifndef fdb5_HAVE_RADOS_STORE_OBJ_PER_FIELD
    const eckit::RadosObject& getDataObject(const Key& key) const;
    eckit::DataHandle& getDataHandle(const Key& key, const eckit::RadosObject& name);
    void closeDataHandles();
    void flushDataHandles();

private: // types

    typedef std::map<Key, eckit::DataHandle*> HandleStore;
    typedef std::map<Key, eckit::RadosObject> ObjectStore;
#endif

private: // members
    
    const Config& config_;

    // mutable bool dirty_;

#ifdef fdb5_HAVE_RADOS_STORE_OBJ_PER_FIELD
  #ifdef fdb5_HAVE_RADOS_STORE_PERSIST_ON_FLUSH
    std::vector<eckit::DataHandle*> handles_;
    size_t maxHandleBuffSize_;
  #endif
#else
    HandleStore handles_;
    mutable ObjectStore dataObjects_;
  #ifdef fdb5_HAVE_RADOS_STORE_PERSIST_ON_FLUSH
    size_t maxAioBuffSize_;
    #ifdef fdb5_HAVE_RADOS_STORE_MULTIPART
    size_t maxPartHandleBuffSize_;
    #endif
  #endif
#endif

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
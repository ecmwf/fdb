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
/// @date   Nov 2022

#pragma once

// #include "fdb5/database/DB.h"
// #include "fdb5/database/Index.h"
#include "fdb5/database/Store.h"
#include "fdb5/rules/Schema.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosStore : public Store {

public: // methods

    DaosStore(const Schema& schema, const Key& key, const Config& config);
    DaosStore(const Schema& schema, const eckit::URI& uri, const Config& config);

    ~DaosStore() override {}

    eckit::URI uri() const override;
    bool uriBelongs(const eckit::URI&) const override;
    bool uriExists(const eckit::URI&) const override;
    eckit::PathName getStoreUnitPath(const eckit::URI&) const override;
    std::vector<eckit::URI> storeUnitURIs() const override;

    bool open() override { return true; }
    void flush() override;
    void close() override {};

    void checkUID() const override { /* nothing to do */ }

protected: // methods

    std::string type() const override { return "daos"; }

    bool exists() const override;

    eckit::DataHandle* retrieve(Field& field) const override;
    FieldLocation* archive(const Key &key, const void *data, eckit::Length length) override;

    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    // eckit::DataHandle *getCachedHandle( const eckit::PathName &path ) const;
    // void closeDataHandles();
    // eckit::DataHandle *createFileHandle(const eckit::PathName &path);
    // eckit::DataHandle *createAsyncHandle(const eckit::PathName &path);
    // eckit::DataHandle *createDataHandle(const eckit::PathName &path);
    // eckit::DataHandle& getDataHandle( const eckit::PathName &path );
    // eckit::PathName generateDataPath(const Key &key) const;
    // eckit::PathName getDataPath(const Key &key);
    // void flushDataHandles();

    void print(std::ostream &out) const override;

private: // types

    // typedef std::map< std::string, eckit::DataHandle * >  HandleStore;
    // typedef std::map< Key, std::string > PathStore;

private: // members

    const Config& config_;

    // HandleStore handles_;    ///< stores the DataHandles being used by the Session

    // PathStore dataPaths_;
    std::string pool_;
    std::string db_str_;

    // mutable bool dirty_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

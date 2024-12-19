/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   RadosStore.h
/// @author Emanuele Danovaro
/// @date   Jan 2020

#ifndef fdb5_RadosStore_H
#define fdb5_RadosStore_H

#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Store.h"
#include "fdb5/rules/Schema.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// Store that implements the FDB on CEPH object store

class RadosStore : public Store {

public: // methods

    RadosStore(const Schema& schema, const Key& key, const Config& config);

    ~RadosStore() override {}

    eckit::URI uri() const override;

    bool open() override { return true; }
    void flush() override;
    void close() override;

    void checkUID() const override { /* nothing to do */ }

protected: // methods

    std::string type() const override { return "rados"; }

    bool exists() const override;

    eckit::DataHandle* retrieve(Field& field, Key& remapKey) const override;
    FieldLocation* archive(const Key& key, const void *data, eckit::Length length) override;

    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    eckit::DataHandle *getCachedHandle( const eckit::PathName &path ) const;
    void closeDataHandles();
    eckit::DataHandle *createFileHandle(const eckit::PathName &path);
    eckit::DataHandle *createAsyncHandle(const eckit::PathName &path);
    eckit::DataHandle *createDataHandle(const eckit::PathName &path);
    eckit::DataHandle& getDataHandle( const eckit::PathName &path );
    eckit::PathName generateDataPath(const Key& key) const;
    eckit::PathName getDataPath(const Key& key);
    void flushDataHandles();

    void print( std::ostream &out ) const override;

private: // types

    typedef std::map< std::string, eckit::DataHandle * >  HandleStore;
    typedef std::map< Key, std::string > PathStore;

private: // members

    HandleStore handles_;    ///< stores the DataHandles being used by the Session

    PathStore   dataPaths_;
    eckit::PathName directory_;

    mutable bool dirty_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif //fdb5_RadosStore_H

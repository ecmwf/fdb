/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Store.h
/// @author Emanuele Danovaro
/// @date   August 2019

#ifndef fdb5_Store_H
#define fdb5_Store_H

#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"

namespace fdb5 {

class Store {
public:

    Store(const Schema& schema) : schema_(schema) {}

    virtual ~Store() {}

    virtual eckit::DataHandle* retrieve(Field& field) const = 0;
    virtual std::unique_ptr<FieldLocation> archive(const Key &key, const void *data, eckit::Length length) = 0;

    virtual void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit = true) const = 0;

    friend std::ostream &operator<<(std::ostream &s, const Store &x);
    virtual void print( std::ostream &out ) const = 0;

    virtual std::string type() const = 0;
    virtual bool open() = 0;
    virtual void flush() = 0;
    virtual void close() = 0;

//    virtual std::string owner() const = 0;
    virtual bool exists() const = 0;
    virtual void checkUID() const = 0;

    virtual bool canMoveTo(const Key& key, const Config& config, const eckit::URI& dest) const;
    virtual void moveTo(const Key& key, const Config& config, const eckit::URI& dest, int threads) const { NOTIMP; }
    virtual void remove(const Key& key) const { NOTIMP; }

    virtual eckit::URI uri() const = 0;

protected: // members
    const Schema& schema_;  //<< schema is owned by catalogue which always outlives the store
};


//----------------------------------------------------------------------------------------------------------------------

class StoreBuilderBase {
    std::string name_;

public:
    StoreBuilderBase(const std::string&);
    virtual ~StoreBuilderBase();
    virtual std::unique_ptr<Store> make(const Schema& schema, const Key& key, const Config& config) = 0;
    virtual std::unique_ptr<Store> make(const Schema& schema, const eckit::URI& uri, const Config& config) = 0;
};

template <class T>
class StoreBuilder : public StoreBuilderBase {
    virtual std::unique_ptr<Store> make(const Schema& schema, const Key& key, const Config& config) override { return std::unique_ptr<T>(new T(schema, key, config)); }
    virtual std::unique_ptr<Store> make(const Schema& schema, const eckit::URI& uri, const Config& config) override { return std::unique_ptr<T>(new T(schema, uri, config)); }

public:
    StoreBuilder(const std::string& name) : StoreBuilderBase(name) {}
    virtual ~StoreBuilder() = default;
};

class StoreFactory {
public:
    static StoreFactory& instance();

    void add(const std::string& name, StoreBuilderBase* builder);
    void remove(const std::string& name);

    bool has(const std::string& name);
    void list(std::ostream&);

    /// @param schema    the schema read by the catalog
    /// @param key       the user-specified key
    /// @param config    the fdb config
    /// @returns         store built by specified builder
    std::unique_ptr<Store> build(const Schema& schema, const Key& key, const Config& config);

    /// @param schema    the schema read by the catalog
    /// @param uri       search uri
    /// @param config    the fdb config
    /// @returns         store built by specified builder
    std::unique_ptr<Store> build(const Schema& schema, const eckit::URI& uri, const Config& config);

private:
    StoreFactory();

    std::map<std::string, StoreBuilderBase*> builders_;
    eckit::Mutex mutex_;
};

}
#endif  // fdb5_Store_H

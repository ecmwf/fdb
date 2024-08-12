/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Catalogue.h
/// @author Emanuele Danovaro
/// @date   August 2019

#pragma once

#include <iosfwd>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "eckit/config/LocalConfiguration.h"
#include "eckit/thread/Mutex.h"

#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/api/helpers/MoveIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/MoveVisitor.h"
#include "fdb5/database/PurgeVisitor.h"
#include "fdb5/database/StatsReportVisitor.h"
#include "fdb5/database/WipeVisitor.h"
#include "fdb5/rules/Schema.h"

namespace fdb5 {

class Store;

typedef std::map<Key, Index> IndexStore;

class Catalogue {
public:

    Catalogue(const Key& key, ControlIdentifiers controlIdentifiers, const fdb5::Config& config)
        : dbKey_(key), config_(config), controlIdentifiers_(controlIdentifiers) {}

    virtual ~Catalogue() {}

    const Key& key() const { return dbKey_; }
    virtual const Key& indexKey() const { NOTIMP; }
    const Config& config() const { return config_; }

    std::unique_ptr<Store> buildStore();
    virtual const Schema& schema() const = 0;

    virtual bool selectIndex(const Key& key) = 0;
    virtual void deselectIndex() = 0;

    virtual std::vector<eckit::PathName> metadataPaths() const = 0;

    virtual void visitEntries(EntryVisitor& visitor, const Store& store, bool sorted = false);

    virtual void hideContents() { NOTIMP; }

    virtual void dump(std::ostream& out, bool simple=false, const eckit::Configuration& conf = eckit::LocalConfiguration()) const = 0;

    virtual StatsReportVisitor* statsReportVisitor() const = 0;
    virtual PurgeVisitor* purgeVisitor(const Store& store) const = 0;
    virtual WipeVisitor* wipeVisitor(const Store& store, const metkit::mars::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const = 0;
    virtual MoveVisitor* moveVisitor(const Store& store, const metkit::mars::MarsRequest& request, const eckit::URI& dest, eckit::Queue<MoveElement>& queue) const = 0;

    virtual void control(const ControlAction& action, const ControlIdentifiers& identifiers) const = 0;

    virtual bool enabled(const ControlIdentifier& controlIdentifier) const;

    virtual std::vector<fdb5::Index> indexes(bool sorted=false) const = 0;

    /// For use by the WipeVisitor
    virtual void maskIndexEntry(const Index& index) const = 0;

    /// For use by purge/wipe
    virtual void allMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata,
                           std::set<eckit::URI>& data) const = 0;

    friend std::ostream &operator<<(std::ostream &s, const Catalogue &x);
    virtual void print( std::ostream &out ) const = 0;

    virtual std::string type() const = 0;
    virtual bool open() = 0;
    virtual void flush() = 0;
    virtual void clean() = 0;
    virtual void close() = 0;

    virtual bool exists() const = 0;
    virtual void checkUID() const = 0;

    virtual eckit::URI uri() const = 0;

protected: // methods

    virtual void loadSchema() = 0;

protected: // members

    Key dbKey_;
    Config config_;
    ControlIdentifiers controlIdentifiers_;

};

class CatalogueReader {
public:
    virtual DbStats stats() const = 0;
    virtual bool axis(const std::string& keyword, eckit::StringSet& s) const = 0;
    virtual bool retrieve(const Key& key, Field& field) const = 0;
};


class CatalogueWriter {
public:
    virtual const Index& currentIndex() = 0;
    virtual void archive(const Key& key, std::shared_ptr<FieldLocation> fieldLocation) = 0;
    virtual void overlayDB(const Catalogue& otherCatalogue, const std::set<std::string>& variableKeys, bool unmount) = 0;
    virtual void index(const Key& key, const eckit::URI& uri, eckit::Offset offset, eckit::Length length) = 0;
    virtual void reconsolidate() = 0;
};

//----------------------------------------------------------------------------------------------------------------------

class CatalogueBuilderBase {
    std::string name_;

public:
    CatalogueBuilderBase(const std::string&);
    virtual ~CatalogueBuilderBase();
    virtual std::unique_ptr<Catalogue> make(const fdb5::Key& key, const fdb5::Config& config) = 0;
    virtual std::unique_ptr<Catalogue> make(const eckit::URI& uri, const fdb5::Config& config) = 0;
};

template <class T>
class CatalogueBuilder : public CatalogueBuilderBase {
    virtual std::unique_ptr<Catalogue> make(const fdb5::Key& key, const fdb5::Config& config) override { return std::unique_ptr<T>(new T(key, config)); }
    virtual std::unique_ptr<Catalogue> make(const eckit::URI& uri, const fdb5::Config& config) override { return std::unique_ptr<T>(new T(uri, config)); }

public:
    CatalogueBuilder(const std::string& name) : CatalogueBuilderBase(name) {}
    virtual ~CatalogueBuilder() = default;
};

class CatalogueFactory {
public:
    static CatalogueFactory& instance();

    void add(const std::string& name, CatalogueBuilderBase* builder);
    void remove(const std::string& name);

    bool has(const std::string& name);
    void list(std::ostream&);

    /// @param db        the db using the required catalogue
    /// @returns         catalogue built by specified builder
    std::unique_ptr<Catalogue> build(const Key& key, const Config& config, bool read);
    std::unique_ptr<Catalogue> build(const eckit::URI& uri, const Config& config, bool read);

private:
    CatalogueFactory();

    std::map<std::string, CatalogueBuilderBase*> builders_;
    eckit::Mutex mutex_;
};

}

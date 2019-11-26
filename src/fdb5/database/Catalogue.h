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

#ifndef fdb5_Catalogue_H
#define fdb5_Catalogue_H

#include <memory>

#include "eckit/config/LocalConfiguration.h"
#include "eckit/types/Types.h"

#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/PurgeVisitor.h"
#include "fdb5/database/StatsReportVisitor.h"
#include "fdb5/database/WipeVisitor.h"
#include "fdb5/rules/Schema.h"

namespace fdb5 {

class Store;

typedef std::map<Key, Index> IndexStore;

class Catalogue {
public:

    Catalogue(const Key& key, const fdb5::Config& config)
        : dbKey_(key), config_(config) {}
    Catalogue(const eckit::URI& uri, const fdb5::Config& config)
        : dbKey_(Key()), config_(config) {}

    virtual ~Catalogue() {}

    const Key& key() const { return dbKey_; }
    const Config& config() const { return config_; }
    virtual const Schema& schema() const = 0;

    virtual bool selectIndex(const Key& key)  = 0;
    virtual void deselectIndex() = 0;
//    virtual const Index& currentIndex() = 0;

    virtual void axis(const std::string& keyword, eckit::StringSet& s) const = 0;

    virtual std::vector<eckit::PathName> metadataPaths() const = 0;

    virtual void visitEntries(EntryVisitor& visitor, const Store& store, bool sorted = false) = 0;

    virtual void hideContents() { NOTIMP; }
//    virtual void overlayDB(const Catalogue& otherCatalogue, const std::set<std::string>& variableKeys, bool unmount) { NOTIMP; }
    virtual void reconsolidateIndexesAndTocs() { NOTIMP; }

    virtual void dump(std::ostream& out, bool simple=false, const eckit::Configuration& conf = eckit::LocalConfiguration()) const = 0;

    virtual StatsReportVisitor* statsReportVisitor() const = 0;
    virtual PurgeVisitor* purgeVisitor() const = 0;
    virtual WipeVisitor* wipeVisitor(const metkit::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const = 0;

    virtual void control(const ControlAction& action, const ControlIdentifiers& identifiers) const = 0;

    virtual bool retrieveLocked() const = 0;
    virtual bool archiveLocked() const = 0;
    virtual bool listLocked() const = 0;
    virtual bool wipeLocked() const = 0;

    virtual std::vector<fdb5::Index> indexes(bool sorted=false) const = 0;

    /// For use by the WipeVisitor
    virtual void maskIndexEntry(const Index& index) const = 0;

    /// For use by purge/wipe
    virtual void allMasked(std::set<std::pair<eckit::PathName, eckit::Offset>>& metadata,
                           std::set<eckit::PathName>& data) const {}

    //virtual void clean() = 0;

    friend std::ostream &operator<<(std::ostream &s, const Catalogue &x);
    virtual void print( std::ostream &out ) const = 0;

    virtual std::string type() const = 0;
    virtual bool open() = 0;
    virtual void flush() = 0;
    virtual void clean() = 0;
    virtual void close() = 0;

//    virtual std::string owner() const = 0;
    virtual bool exists() const = 0;
    virtual void checkUID() const = 0;

    virtual eckit::URI uri() const = 0;

//    friend class DB;

    // from DBCommon
/*    void checkUID() const override {NOTIMP;}
    std::string type() const override {NOTIMP;}
    std::string owner() const override {NOTIMP;}
    bool exists() const override {NOTIMP;}

    bool open() override {NOTIMP;}
    void flush() override {NOTIMP;}
    void close() override {NOTIMP;}

protected: // methods

    // from DBCommon
    void print( std::ostream &out ) const override {NOTIMP;}
*/
protected: // methods

    virtual void loadSchema() = 0;

protected: // members

    Key dbKey_;
    const Config& config_;

};

class CatalogueReader {
public:
    virtual DbStats stats() const = 0;
    virtual bool retrieve(const Key& key, Field& field, Key& remapKey) const = 0;
};


class CatalogueWriter {
public:
    virtual const Index& currentIndex() = 0;
    virtual void archive(const Key& key, const FieldLocation* fieldLocation) = 0;
    virtual void overlayDB(const Catalogue& otherCatalogue, const std::set<std::string>& variableKeys, bool unmount) = 0;
    virtual void index(const Key &key, const eckit::PathName &path, eckit::Offset offset, eckit::Length length) = 0;
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
    virtual std::unique_ptr<Catalogue> make(const fdb5::Key& key, const fdb5::Config& config) { return std::unique_ptr<T>(new T(key, config)); }
    virtual std::unique_ptr<Catalogue> make(const eckit::URI& uri, const fdb5::Config& config) { return std::unique_ptr<T>(new T(uri, config)); }

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
#endif  // fdb5_Catalogue_H

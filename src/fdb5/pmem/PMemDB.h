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
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @file   PMemDB.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_PMemDB_H
#define fdb5_PMemDB_H

#include <memory>

#include "fdb5/config/Config.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"

#include "fdb5/pmem/DataPoolManager.h"
#include "fdb5/pmem/PIndexRoot.h"
#include "fdb5/pmem/PMemEngine.h"
#include "fdb5/pmem/Pool.h"

namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on POSIX filesystems

class PMemDB : public DB {

public: // methods

    PMemDB(const Key& key, const Config& config);
    PMemDB(const eckit::PathName& directory, const Config& config);

    virtual ~PMemDB();

    static const char* dbTypeName() { return PMemEngine::typeName(); }

    size_t schemaSize() const;

    /// How many data pools are under management?
    size_t dataPoolsCount() const;

    /// The total size of all of the pools involved in this database
    size_t dataPoolsSize() const;

protected: // methods

    virtual std::string dbType() const;

    void checkUID() const override;

    bool open() override;
    void close() override;
    void flush() override;
    bool exists() const override;
    void visitEntries(EntryVisitor& visitor, bool sorted=false) override;
    void visit(DBVisitor& visitor) override;
    void dump(std::ostream& out, bool simple=false) const override;
    std::string owner() const override;
    eckit::PathName basePath() const override;
    std::vector<eckit::PathName> metadataPaths() const;
    const Schema& schema() const override;

    virtual eckit::DataHandle *retrieve(const Key &key) const;
    virtual void archive(const Key &key, const void *data, eckit::Length length);
    virtual void axis(const std::string &keyword, eckit::StringSet &s) const;

    virtual StatsReportVisitor* statsReportVisitor() const override;
    virtual PurgeVisitor* purgeVisitor() const override;
    virtual void maskIndexEntry(const Index& index) const override;

    // void loadSchema();

    virtual bool selectIndex(const Key &key);
    virtual void deselectIndex();

    virtual DbStats statistics() const;

    virtual std::vector<Index> indexes(bool sorted=false) const;

    // Control access properties of the DB

    void control(const ControlAction& action, const ControlIdentifiers& identifiers) override { NOTIMP; }

    bool retrieveLocked() const override { NOTIMP; }
    bool archiveLocked() const override { NOTIMP; }
    bool listLocked() const override { NOTIMP; }
    bool wipeLocked() const override { NOTIMP; }

private: // methods

    /// Initialise or open the peristent pool. Worker function for the construtor
    void initialisePool();

protected: // types

    typedef std::map<Key, Index> IndexStore;

protected: // members

    eckit::PathName poolDir_;
    Config dbConfig_;

    std::unique_ptr<Pool> pool_;

    // Not owned by PMemDB but by pool_. This is only a pointer not a reference so it can
    // be initialised later than the PMemDB is constructed.
    PIndexRoot* root_;

    std::unique_ptr<DataPoolManager> dataPoolMgr_;

    IndexStore  indexes_;
    Index currentIndex_;

    Schema schema_;

    bool init_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif

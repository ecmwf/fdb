/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   DB.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_DB_H
#define fdb5_DB_H

#include <iosfwd>

#include "eckit/memory/Owned.h"
#include "eckit/io/Length.h"
#include "eckit/types/Types.h"
#include "eckit/config/LocalConfiguration.h"

#include "fdb5/database/Key.h"

namespace eckit {
class DataHandle;
class PathName;
}

class MarsTask;

namespace fdb5 {

class Key;
class Index;
class EntryVisitor;
class StatsReportVisitor;
class Schema;

class DBVisitor;
class DbStats;

//----------------------------------------------------------------------------------------------------------------------

class DB : public eckit::OwnedLock {

public: // methods

    DB(const Key &key);

    virtual ~DB();

    const Key& key() const { return dbKey_; }

    virtual std::string dbType() const = 0;

    virtual bool selectIndex(const Key &key) = 0;
    virtual void deselectIndex() = 0;

    virtual bool open() = 0;

    virtual void axis(const std::string &keyword, eckit::StringSet &s) const = 0;

    virtual eckit::DataHandle *retrieve(const Key &key) const = 0;

    virtual void archive(const Key &key, const void *data, eckit::Length length) = 0;

    virtual void flush() = 0;

    virtual void close() = 0;

    virtual void checkSchema(const Key &key) const = 0;

    virtual bool exists() const = 0;

    /// If sorted is specified, the entries may be visited in the most efficient order, rather than
    /// in the logical read order implied by the appends
    /// (i.e. for Toc, visit indexes in the order they are stored, file by file).
    virtual void visitEntries(EntryVisitor& visitor, bool sorted=false) = 0;

    virtual void visit(DBVisitor& visitor) = 0;

    virtual void dump(std::ostream& out, bool simple=false) = 0;

    virtual StatsReportVisitor* statsReportVisitor();

    virtual std::string owner() const = 0;

    virtual eckit::PathName basePath() const = 0;

    virtual const Schema& schema() const = 0;

    virtual DbStats statistics() const = 0;

    friend std::ostream &operator<<(std::ostream &s, const DB &x);

    time_t lastAccess() const;

    void touch();

    /// @returns all the indexes in this DB
    virtual std::vector<fdb5::Index> indexes(bool sorted=false) const = 0;

protected: // methods

    virtual void print( std::ostream &out ) const = 0;

protected: // members

    Key dbKey_;

    time_t lastAccess_;

};

//----------------------------------------------------------------------------------------------------------------------

/// A self-registering factory for producing DB instances.

class DBFactory : private eckit::NonCopyable {

    std::string name_;
    bool read_;
    bool write_;

    virtual DB *make(const Key &key, const eckit::Configuration& config) const = 0 ;
    virtual DB *make(const eckit::PathName& path, const eckit::Configuration& config) const = 0 ;

protected:

    DBFactory(const std::string &, bool read, bool write);
    virtual ~DBFactory();

public:

    static void list(std::ostream &);
    static DB* buildWriter(const Key &key, const eckit::Configuration& config=eckit::LocalConfiguration());
    static DB* buildReader(const Key &key, const eckit::Configuration& config=eckit::LocalConfiguration());
    static DB* buildReader(const eckit::PathName& path, const eckit::Configuration& config=eckit::LocalConfiguration());

private: // methods

    static const DBFactory &findFactory(const std::string &);

    bool read() const;
    bool write() const;
};

/// Templated specialisation of the self-registering factory,
/// that does the self-registration, and the construction of each object.

template< class T>
class DBBuilder : public DBFactory {

    virtual DB *make(const Key &key, const eckit::Configuration& config) const {
        return new T(key, config);
    }
    virtual DB *make(const eckit::PathName& path, const eckit::Configuration& config) const {
        return new T(path, config);
    }

public:
    DBBuilder(const std::string &name, bool read, bool write) : DBFactory(name, read, write) {}
};


//----------------------------------------------------------------------------------------------------------------------

class DBVisitor : private eckit::NonCopyable {
public:

    virtual ~DBVisitor();
    virtual void operator() (DB& db) = 0;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

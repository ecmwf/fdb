/*
 * (C) Copyright 1996-2016 ECMWF.
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
#include "eckit/memory/SharedPtr.h"
#include "eckit/io/Length.h"
#include "eckit/types/Types.h"

#include "fdb5/Schema.h"
#include "fdb5/Key.h"

namespace eckit { class DataHandle; }

class MarsTask;

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class DB : public eckit::OwnedLock {

public: // methods

    DB(const Key& key);

    virtual ~DB();

    const Key& key() const { return key_; }

    virtual bool selectIndex(const Key& key) = 0;

    virtual bool open() = 0;

    virtual void axis(const Key& key, const std::string& keyword, eckit::StringSet& s) const = 0;

    virtual eckit::DataHandle* retrieve(const Key& key) const = 0;

    virtual void archive(const Key& key, const void* data, eckit::Length length) = 0;

    virtual void flush() = 0;

    virtual void close() = 0;

    friend std::ostream& operator<<(std::ostream& s,const DB& x);

protected: // methods

    virtual void print( std::ostream& out ) const = 0;

protected: // members

    Key key_;

};

typedef std::vector< eckit::SharedPtr< DB > > VecDB;

//----------------------------------------------------------------------------------------------------------------------

/// A self-registering factory for producing DB instances.

class DBFactory {

    std::string name_;

    virtual DB* make(const Key& key) const = 0 ;

protected:

    DBFactory(const std::string&);
    virtual ~DBFactory();

public:

    static void list(std::ostream &);
    static DB* build(const std::string&, const Key &key);

private: // methods

    static const DBFactory& findFactory(const std::string&);
};

/// Templated specialisation of the self-registering factory,
/// that does the self-registration, and the construction of each object.

template< class T>
class DBBuilder : public DBFactory {

    virtual DB* make(const Key& key) const {
        return new T(key);
    }

public:
    DBBuilder(const std::string &name) : DBFactory(name) {}
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

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

#ifndef fdb_DB_H
#define fdb_DB_H

#include <vector>
#include <string>
#include <iosfwd>

#include "eckit/memory/Owned.h"
#include "eckit/memory/SharedPtr.h"

namespace eckit { class DataHandle; }
namespace marskit { class MarsRequest; }

namespace fdb {

class FdbTask;

//----------------------------------------------------------------------------------------------------------------------

class DB : public eckit::OwnedLock {

public: // methods

	DB();

    virtual ~DB();

    virtual std::vector<std::string> schema() const = 0;

    virtual eckit::DataHandle* retrieve(const FdbTask& task, const marskit::MarsRequest& field) const = 0;

    friend std::ostream& operator<<(std::ostream& s,const DB& x);

protected: // methods

    virtual void print( std::ostream& out ) const = 0;

};

typedef std::vector< eckit::SharedPtr< DB > > VecDB;

//----------------------------------------------------------------------------------------------------------------------

/// A self-registering factory for producing DB instances.

class DBFactory {

    std::string name_;

    virtual DB* make() const = 0 ;

protected:

    DBFactory(const std::string&);
    virtual ~DBFactory();

public:

    static void list(std::ostream &);
    static DB* build(const std::string&);

private: // methods

    static const DBFactory& findFactory(const std::string&);
};

/// Templated specialisation of the self-registering factory,
/// that does the self-registration, and the construction of each object.

template< class T>
class DBBuilder : public DBFactory {

    virtual DB* make() const {
        return new T();
    }

public:
    DBBuilder(const std::string &name) : DBFactory(name) {}
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

#endif

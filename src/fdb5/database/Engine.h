/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @author Simon Smart
/// @date   Jan 2017

#ifndef fdb5_Engine_H
#define fdb5_Engine_H

#include <iosfwd>
#include <vector>
#include <string>

#include "eckit/memory/NonCopyable.h"
#include "eckit/filesystem/PathName.h"

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class Engine : private eckit::NonCopyable {

public: // methods

    static Engine& backend(const std::string& name);

public: // methods

    virtual ~Engine();

    /// @returns the named identifier of this engine
    virtual std::string name() const = 0;
    virtual std::string dbType() const = 0;

    /// Uniquely selects a location where the Key will be put or already exists
    virtual eckit::PathName location(const Key &key) const = 0;

    /// Lists the roots that can be visited given a DB key
    virtual std::vector<eckit::PathName> allLocations(const Key& key) const = 0;

    /// Lists the roots that can be visited given a DB key
    virtual std::vector<eckit::PathName> visitableLocations(const Key& key) const = 0;

    /// Lists the roots where a DB key would be able to be written
    virtual std::vector<eckit::PathName> writableLocations(const Key& key) const = 0;

    friend std::ostream &operator<<(std::ostream &s, const Engine& x);

protected: // methods

    virtual void print( std::ostream &out ) const = 0;

};

//----------------------------------------------------------------------------------------------------------------------

/// A self-registering factory for producing Engine instances.

class EngineFactory : private eckit::NonCopyable {

    std::string name_;

    virtual Engine *make() const = 0 ;

protected:

    EngineFactory(const std::string& name);
    virtual ~EngineFactory();

public:

    static std::vector<std::string> list();
    static void list(std::ostream &);
    static Engine* build(const std::string&);

private: // methods

    static const EngineFactory& findFactory(const std::string&);
};

/// Templated for self-registering factory that does the self-registration, and the construction of each object.

template< class T>
class EngineBuilder : public EngineFactory {

    virtual Engine *make() const {
        return new T();
    }

public:
    EngineBuilder(const std::string &name) : EngineFactory(name) {}
};


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

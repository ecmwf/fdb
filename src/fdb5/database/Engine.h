/*
 * (C) Copyright 1996- ECMWF.
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
#include <string>
#include <vector>

#include "metkit/mars/MarsRequest.h"

#include "eckit/filesystem/URI.h"
#include "eckit/memory/NonCopyable.h"


namespace fdb5 {

class Key;
class Config;

//----------------------------------------------------------------------------------------------------------------------

class Engine : private eckit::NonCopyable {

public:  // methods

    static Engine& backend(const std::string& name);

public:  // methods

    virtual ~Engine();

    /// @returns the named identifier of this engine
    virtual std::string name() const   = 0;
    virtual std::string dbType() const = 0;

    /// @returns if an Engine is capable of opening this path
    virtual bool canHandle(const eckit::URI& uri, const Config&) const = 0;

    /// Uniquely selects a location where the Key will be put or already exists
    virtual eckit::URI location(const Key& key, const Config& config) const = 0;

    /// Lists the roots that can be visited given a DB key
    virtual std::vector<eckit::URI> visitableLocations(const Key& key, const Config& config) const = 0;
    virtual std::vector<eckit::URI> visitableLocations(const metkit::mars::MarsRequest& rq,
                                                       const Config& config) const                 = 0;

    friend std::ostream& operator<<(std::ostream& s, const Engine& x);

protected:  // methods

    virtual void print(std::ostream& out) const = 0;
};

//----------------------------------------------------------------------------------------------------------------------

/// A self-registering registry for Engine instances

class EngineRegistry : private eckit::NonCopyable {

public:  // methods

    static bool has(const std::string& name);

    static Engine& engine(const std::string& name);

    static std::vector<Engine*> engines();

    static std::vector<std::string> list();

    static void list(std::ostream&);

protected:  // methods

    static void add(Engine*);
    static Engine* remove(const std::string&);
};


/// Templated for self-registering engines that does the self-registration into the registry

template <class T>
class EngineBuilder : public EngineRegistry {
public:

    EngineBuilder() {
        Engine* e = new T();
        name_     = e->name();
        EngineRegistry::add(e);
    }

    ~EngineBuilder() {
        Engine* e = EngineRegistry::remove(name_);
        delete e;
    }

    std::string name_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

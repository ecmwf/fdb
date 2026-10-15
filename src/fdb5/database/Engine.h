// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @author Simon Smart
/// @date   Jan 2017

#pragma once

#include <iosfwd>
#include <string>
#include <vector>

#include "metkit/mars/MarsRequest.h"

#include "eckit/filesystem/URI.h"


namespace fdb5 {

class Key;
class Config;

//----------------------------------------------------------------------------------------------------------------------

class Engine {

public:  // methods

    static Engine& backend(const std::string& name);

public:  // methods

    Engine() = default;

    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;
    Engine(Engine&&) = delete;
    Engine& operator=(Engine&&) = delete;

    virtual ~Engine();

    /// @returns the named identifier of this engine
    virtual std::string name() const = 0;
    virtual std::string dbType() const = 0;

    /// @returns if an Engine is capable of opening this path
    virtual bool canHandle(const eckit::URI& uri, const Config&) const = 0;

    /// Uniquely selects a location where the Key will be put or already exists
    virtual eckit::URI location(const Key& key, const Config& config) const = 0;

    /// Lists the roots that can be visited given a DB key
    virtual std::vector<eckit::URI> visitableLocations(const Key& key, const Config& config) const = 0;
    virtual std::vector<eckit::URI> visitableLocations(const metkit::mars::MarsRequest& rq,
                                                       const Config& config) const = 0;

    friend std::ostream& operator<<(std::ostream& s, const Engine& x);

protected:  // methods

    virtual void print(std::ostream& out) const = 0;
};

//----------------------------------------------------------------------------------------------------------------------

/// A self-registering registry for Engine instances

class EngineRegistry {

public:  // methods

    EngineRegistry() = default;  // Necessary, because this is a base class!

    EngineRegistry(const EngineRegistry&) = delete;
    EngineRegistry& operator=(const EngineRegistry&) = delete;
    EngineRegistry(EngineRegistry&&) = delete;
    EngineRegistry& operator=(EngineRegistry&&) = delete;

    virtual ~EngineRegistry() = default;  // Necessary, because this is a base class!

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
        name_ = e->name();
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


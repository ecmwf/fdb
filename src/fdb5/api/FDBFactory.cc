/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "eckit/thread/Mutex.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/log/Log.h"

#include "fdb5/api/FDBFactory.h"
#include "fdb5/LibFdb.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


FDBBase::FDBBase(const Config &config) :
    config_(config) {}


FDBBase::~FDBBase() {}


static eckit::Mutex& factoryMutex() {
    static eckit::Mutex m;
    return m;
}


static std::map<std::string, const FDBFactory*>& factoryRegistry() {
    static std::map<std::string, const FDBFactory*> registry;
    return registry;
}



FDBFactory::FDBFactory(const std::string &name) :
    name_(name) {

    eckit::AutoLock<eckit::Mutex> lock(factoryMutex());

    auto& registry = factoryRegistry();

    auto it = registry.find(name);
    if (it != registry.end()) {
        std::stringstream ss;
        ss << "FDB factory \"" << name << "\" already registered";
        throw eckit::SeriousBug(ss.str(), Here());
    }

    registry[name] = this;
}

FDBFactory::~FDBFactory() {

    eckit::AutoLock<eckit::Mutex> lock(factoryMutex());
    auto& registry = factoryRegistry();

    auto it = registry.find(name_);
    ASSERT(it != registry.end());

    registry.erase(it);
}

std::unique_ptr<FDBBase> FDBFactory::build(const Config& config) {

    /// We use "local" as a default type if not otherwise configured.

    std::string key = config.getString("type", "local");

    eckit::Log::debug<LibFdb>() << "Selecting FDB implementation: " << key << std::endl;

    const FDBFactory* factory = nullptr;

    eckit::AutoLock<eckit::Mutex> lock(factoryMutex());
    auto& registry = factoryRegistry();

    auto it = registry.find(key);

    if (it == registry.end()) {
        std::stringstream ss;
        ss << "FDB factory \"" << key << "\" not found";
        throw eckit::SeriousBug(ss.str(), Here());
    }

    std::unique_ptr<FDBBase> ret = it->second->make(config);
    eckit::Log::debug<LibFdb>() << "Constructed FDB implementation: " << *ret << std::endl;
    return ret;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/thread/AutoLock.h"
#include "eckit/thread/Mutex.h"
#include "eckit/filesystem/PathName.h"

#include "fdb5/database/Engine.h"

using eckit::AutoLock;
using eckit::Mutex;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static std::map<std::string, EngineFactory *> *m = 0;

class EngineRegistry {
public:

    typedef std::map<std::string, Engine*> Map;

    Engine& backend(const std::string& name) {

        AutoLock<Mutex> lock(mutex_);

        Map::iterator itr = map_.find(name);
        if(itr != map_.end())
            return *(itr->second);

        Engine* e = EngineFactory::build(name);

        map_[name] = e;

        return *e;
    }

private: // members

    Map map_;
    mutable Mutex mutex_;
};

static EngineRegistry* engines = 0;

eckit::Mutex *local_mutex = 0;
pthread_once_t once = PTHREAD_ONCE_INIT;
void init() {
    local_mutex = new eckit::Mutex();
    m = new std::map<std::string, EngineFactory *>();
    engines = new EngineRegistry();
}

//----------------------------------------------------------------------------------------------------------------------

/// When a concrete instance of a EngineFactory is instantiated (in practice a EngineBuilder<>)
/// add it to the list of available factories.

EngineFactory::EngineFactory(const std::string& name) :
    name_(name) {

    pthread_once(&once, init);

    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    ASSERT(m->find(name) == m->end());
    (*m)[name] = this;
}

EngineFactory::~EngineFactory() {
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);
    m->erase(name_);
}

std::vector<std::string> EngineFactory::list()
{
    std::vector<std::string> res;
    for (std::map<std::string, EngineFactory *>::const_iterator j = m->begin(); j != m->end(); ++j) {
        res.push_back(j->first);
    }
    return res;
}

void EngineFactory::list(std::ostream &out) {

    pthread_once(&once, init);

    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    const char *sep = "";
    for (std::map<std::string, EngineFactory *>::const_iterator j = m->begin(); j != m->end(); ++j) {
        out << sep << (*j).first;
        sep = ", ";
    }
}


const EngineFactory &EngineFactory::findFactory(const std::string &name) {

    pthread_once(&once, init);

    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    eckit::Log::info() << "Looking for FDB EngineFactory [" << name << "]" << std::endl;

    std::map<std::string, EngineFactory *>::const_iterator j = m->find(name);
    if (j == m->end()) {
        eckit::Log::error() << "No FDB EngineFactory for [" << name << "]" << std::endl;
        eckit::Log::error() << "DBFactories are:" << std::endl;
        for (j = m->begin() ; j != m->end() ; ++j)
            eckit::Log::error() << "   " << (*j).first << std::endl;
        throw eckit::SeriousBug(std::string("No EngineFactory called ") + name);
    }

    return *(*j).second;
}


Engine *EngineFactory::build(const std::string& name) {
    const EngineFactory& factory( findFactory(name) );
    return factory.make();
}

//----------------------------------------------------------------------------------------------------------------------

Engine& Engine::backend(const std::string& name)
{
    pthread_once(&once, init);

    return engines->backend(name);
}

Engine::~Engine() {
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

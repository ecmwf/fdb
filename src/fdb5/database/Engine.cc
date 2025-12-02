/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/Mutex.h"

#include "fdb5/database/Engine.h"

using eckit::AutoLock;
using eckit::Mutex;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

eckit::Mutex* local_mutex = 0;
pthread_once_t once = PTHREAD_ONCE_INIT;

static std::map<std::string, Engine*>* m;

void init() {
    local_mutex = new eckit::Mutex();
    m = new std::map<std::string, Engine*>();
}

bool EngineRegistry::has(const std::string& name) {
    pthread_once(&once, init);
    AutoLock<Mutex> lock(*local_mutex);

    return (m->find(name) != m->end());
}

Engine& EngineRegistry::engine(const std::string& name) {

    pthread_once(&once, init);
    AutoLock<Mutex> lock(*local_mutex);

    std::map<std::string, Engine*>::iterator i = m->find(name);
    if (i != m->end()) {
        return *(i->second);
    }

    std::ostringstream oss;
    oss << "No FDB Engine registered with name " << name;

    throw eckit::BadParameter(oss.str(), Here());
}

std::vector<Engine*> EngineRegistry::engines() {
    pthread_once(&once, init);
    AutoLock<Mutex> lock(*local_mutex);

    std::vector<Engine*> res;
    for (std::map<std::string, Engine*>::const_iterator j = m->begin(); j != m->end(); ++j) {
        res.push_back(j->second);
    }
    return res;
}

void EngineRegistry::add(Engine* e) {
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(*local_mutex);

    std::string name = e->name();

    ASSERT(m->find(name) == m->end());
    (*m)[name] = e;
}

Engine* EngineRegistry::remove(const std::string& name) {
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(*local_mutex);

    Engine* e = (*m)[name];

    m->erase(name);

    return e;
}


std::vector<std::string> EngineRegistry::list() {
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(*local_mutex);

    std::vector<std::string> res;
    for (std::map<std::string, Engine*>::const_iterator j = m->begin(); j != m->end(); ++j) {
        res.push_back(j->first);
    }
    return res;
}

void EngineRegistry::list(std::ostream& out) {

    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    const char* sep = "";
    for (std::map<std::string, Engine*>::const_iterator j = m->begin(); j != m->end(); ++j) {
        out << sep << (*j).first;
        sep = ", ";
    }
}

//----------------------------------------------------------------------------------------------------------------------

Engine& Engine::backend(const std::string& name) {
    return EngineRegistry::engine(name);
}

Engine::~Engine() {}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

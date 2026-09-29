/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/types/TypesFactory.h"

#include <pthread.h>

#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/Mutex.h"

#include "fdb5/LibFdb5.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static eckit::Mutex* local_mutex = 0;
static std::map<std::string, TypesFactory*>* m = 0;
static pthread_once_t once = PTHREAD_ONCE_INIT;
static void init() {
    local_mutex = new eckit::Mutex();
    m = new std::map<std::string, TypesFactory*>();
}

//----------------------------------------------------------------------------------------------------------------------

TypesFactory::TypesFactory(const std::string& name) : name_(name) {
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    ASSERT(m->find(name) == m->end());
    (*m)[name] = this;
}

TypesFactory::~TypesFactory() {
    if (LibFdb5::instance().dontDeregisterFactories()) {
        return;
    }
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);
    m->erase(name_);
}

Type* TypesFactory::build(const std::string& name, const std::string& keyword) {
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    std::map<std::string, TypesFactory*>::const_iterator j = m->find(name);

    if (j == m->end()) {
        eckit::Log::error() << "No TypesFactory for [" << name << "]" << std::endl;
        eckit::Log::error() << "KeywordTypes are:" << std::endl;
        for (j = m->begin(); j != m->end(); ++j) {
            eckit::Log::error() << "   " << (*j).first << std::endl;
        }
        throw eckit::SeriousBug(std::string("No TypesFactory called ") + name);
    }

    return (*j).second->make(keyword);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

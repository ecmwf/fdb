/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/thread/AutoLock.h"

#include "fdb5/DB.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {
    Mutex* local_mutex = 0;
    std::map<std::string, DBFactory*> *m = 0;
    pthread_once_t once = PTHREAD_ONCE_INIT;
    void init() {
        local_mutex = new Mutex();
        m = new std::map<std::string, DBFactory*>();
    }
}

/// When a concrete instance of a DBFactory is instantiated (in practice
/// a DBBuilder<>) add it to the list of available factories.

DBFactory::DBFactory(const std::string& name) :
    name_(name) {

    pthread_once(&once, init);

    AutoLock<Mutex> lock(local_mutex);

    ASSERT(m->find(name) == m->end());
    (*m)[name] = this;
}

DBFactory::~DBFactory() {
    AutoLock<Mutex> lock(local_mutex);
    m->erase(name_);
}

void DBFactory::list(std::ostream& out) {

    pthread_once(&once, init);

    AutoLock<Mutex> lock(local_mutex);

    const char* sep = "";
    for (std::map<std::string, DBFactory*>::const_iterator j = m->begin(); j != m->end(); ++j) {
        out << sep << (*j).first;
        sep = ", ";
    }
}


const DBFactory& DBFactory::findFactory(const std::string& name) {

    pthread_once(&once, init);

    AutoLock<Mutex> lock(local_mutex);

    Log::info() << "Looking for DBFactory [" << name << "]" << std::endl;

    std::map<std::string, DBFactory *>::const_iterator j = m->find(name);
    if (j == m->end()) {
        Log::error() << "No DBFactory for [" << name << "]" << std::endl;
        Log::error() << "DBFactories are:" << std::endl;
        for (j = m->begin() ; j != m->end() ; ++j)
            Log::error() << "   " << (*j).first << std::endl;
        throw SeriousBug(std::string("No DBFactory called ") + name);
    }

    return *(*j).second;
}


DB* DBFactory::build(const std::string& name, const Key& key) {

    const DBFactory& factory( findFactory(name) );

    return factory.make(key);
}

//----------------------------------------------------------------------------------------------------------------------

DB::DB(const Key& key)
{
}

DB::~DB()
{
}

std::ostream& operator<<(std::ostream& s, const DB& x)
{
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

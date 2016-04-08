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
#include "eckit/exception/Exceptions.h"
#include "eckit/config/Resource.h"

#include "marslib/MarsTask.h"

#include "fdb5/KeywordType.h"
#include "fdb5/DefaultKeywordHandler.h"

#include "fdb5/Op.h"
#include "fdb5/ForwardOp.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static eckit::Mutex *local_mutex = 0;
static std::map<std::string, KeywordType*> *m = 0;
static pthread_once_t once = PTHREAD_ONCE_INIT;
static void init() {
    local_mutex = new eckit::Mutex();
    m = new std::map<std::string, KeywordType *>();
}

//----------------------------------------------------------------------------------------------------------------------

KeywordType::KeywordType(const std::string& name) :
    name_(name)
{
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    Log::debug() << "Inserting handler " << name << std::endl;

    ASSERT(m->find(name) == m->end());
    (*m)[name] = this;
}

KeywordType::~KeywordType() {
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);
    m->erase(name_);
}

KeywordHandler* KeywordType::build(const std::string& name, const std::string& keyword)
{
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    std::map<std::string, KeywordType*>::const_iterator j = m->find(name);

    if (j == m->end()) {
        Log::error() << "No KeywordType for [" << name << "]" << std::endl;
        Log::error() << "KeywordTypes are:" << std::endl;
        for (j = m->begin() ; j != m->end() ; ++j)
            Log::error() << "   " << (*j).first << std::endl;
        throw SeriousBug(std::string("No KeywordType called ") + name);
    }

    return (*j).second->make(keyword);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

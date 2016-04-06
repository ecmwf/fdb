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

#include "fdb5/KeywordHandler.h"
#include "fdb5/DefaultKeywordHandler.h"

#include "fdb5/Op.h"
#include "fdb5/ForwardOp.h"
#include "fdb5/UVOp.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static eckit::Mutex *local_mutex = 0;
static std::map<std::string, KeywordHandler*> *m = 0;
static pthread_once_t once = PTHREAD_ONCE_INIT;
static void init() {
    local_mutex = new eckit::Mutex();
    m = new std::map<std::string, KeywordHandler *>();
}

//----------------------------------------------------------------------------------------------------------------------

KeywordHandler::KeywordHandler(const std::string& name) :
    name_(name)
{
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    Log::debug() << "Inserting handler " << name << std::endl;

    ASSERT(m->find(name) == m->end());
    (*m)[name] = this;
}

KeywordHandler::~KeywordHandler() {
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);
    m->erase(name_);
}

const KeywordHandler& KeywordHandler::lookup(const std::string& keyword)
{
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    std::map<std::string, KeywordHandler*>::const_iterator j = m->find(keyword);

    if (j != m->end()) {
        return *(*j).second;
    }
    else {
        return lookup(DefaultKeywordHandler::name());
    }
}

std::ostream& operator<<(std::ostream& s, const KeywordHandler& x)
{
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

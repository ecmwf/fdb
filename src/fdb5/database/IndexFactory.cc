/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/IndexFactory.h"

#include <map>
#include <ostream>
#include <string>

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/log/Log.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/Mutex.h"

#include "fdb5/LibFdb5.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static eckit::Mutex* local_mutex = 0;
static std::map<std::string, BTreeIndexFactory*>* m = 0;
static pthread_once_t once = PTHREAD_ONCE_INIT;
static void init() {
    local_mutex = new eckit::Mutex();
    m = new std::map<std::string, BTreeIndexFactory*>();
}

//----------------------------------------------------------------------------------------------------------------------

BTreeIndexFactory::BTreeIndexFactory(const std::string& name) : name_(name) {
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    ASSERT(m->find(name) == m->end());
    (*m)[name] = this;
}

BTreeIndexFactory::~BTreeIndexFactory() {
    if (LibFdb5::instance().dontDeregisterFactories()) {
        return;
    }
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);
    m->erase(name_);
}

BTreeIndex* BTreeIndexFactory::build(const std::string& name, const eckit::PathName& path, bool readOnly,
                                     off_t offset) {
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    std::map<std::string, BTreeIndexFactory*>::const_iterator j = m->find(name);

    if (j == m->end()) {
        eckit::Log::error() << "No IndexFactory for [" << name << "]" << std::endl;
        eckit::Log::error() << "Values are:" << std::endl;
        for (j = m->begin(); j != m->end(); ++j) {
            eckit::Log::error() << "   " << (*j).first << std::endl;
        }
        throw eckit::SeriousBug(std::string("No IndexFactory called ") + name);
    }

    return (*j).second->make(path, readOnly, offset);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

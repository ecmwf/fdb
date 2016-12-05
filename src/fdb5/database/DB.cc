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
#include "eckit/filesystem/PathName.h"

#include "fdb5/database/DB.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {
eckit::Mutex *local_mutex = 0;
std::map<std::string, DBFactory *> *m = 0;
pthread_once_t once = PTHREAD_ONCE_INIT;
void init() {
    local_mutex = new eckit::Mutex();
    m = new std::map<std::string, DBFactory *>();
}
}

/// When a concrete instance of a DBFactory is instantiated (in practice
/// a DBBuilder<>) add it to the list of available factories.

DBFactory::DBFactory(const std::string &name, bool read, bool write) :
    name_(name),
    read_(read),
    write_(write) {

    pthread_once(&once, init);

    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    ASSERT(m->find(name) == m->end());
    (*m)[name] = this;
}

DBFactory::~DBFactory() {
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);
    m->erase(name_);
}

void DBFactory::list(std::ostream &out) {

    pthread_once(&once, init);

    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    const char *sep = "";
    for (std::map<std::string, DBFactory *>::const_iterator j = m->begin(); j != m->end(); ++j) {
        out << sep << (*j).first;
        sep = ", ";
    }
}


const DBFactory &DBFactory::findFactory(const std::string &name) {

    pthread_once(&once, init);

    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    eckit::Log::info() << "Looking for DBFactory [" << name << "]" << std::endl;

    std::map<std::string, DBFactory *>::const_iterator j = m->find(name);
    if (j == m->end()) {
        eckit::Log::error() << "No DBFactory for [" << name << "]" << std::endl;
        eckit::Log::error() << "DBFactories are:" << std::endl;
        for (j = m->begin() ; j != m->end() ; ++j)
            eckit::Log::error() << "   " << (*j).first << std::endl;
        throw eckit::SeriousBug(std::string("No DBFactory called ") + name);
    }

    return *(*j).second;
}


DB *DBFactory::build(const std::string &name, const Key &key) {

    const DBFactory &factory( findFactory(name) );

    return factory.make(key);
}


/// If we have been supplied a path to a DB, but don't know what type it is, then we
/// need to try and open it with the different DBs, and see how they cope.
///
DB *DBFactory::build_read(const eckit::PathName& path) {

    pthread_once(&once, init);

    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    eckit::Log::info() << "Trying to open DB at path: " << path << std::endl;

    std::map<std::string, DBFactory*>::const_iterator j;
    for (j = m->begin(); j != m->end(); ++j) {

        if (j->second->read()) {

            eckit::Log::info() << "Read DB found: " << j->first << std::endl;

            DB* db = j->second->make(path);
            if (db != 0) {
                if (!db->exists())
                    delete db;
                else
                    return db;
            }
        }
    }

    throw eckit::SeriousBug(std::string("No appropriate DBFactory found for path") + path);
}


bool DBFactory::read() const {
    return read_;
}

bool DBFactory::write() const {
    return write_;
}

//----------------------------------------------------------------------------------------------------------------------

DB::DB(const Key &key) :
    dbKey_(key) {
    touch();
}

DB::~DB() {
}

time_t DB::lastAccess() const {
    return lastAccess_;
}

void DB::touch() {
    lastAccess_ = ::time(0);
}

std::ostream &operator<<(std::ostream &s, const DB &x) {
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

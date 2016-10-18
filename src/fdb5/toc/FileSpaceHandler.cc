/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/FileSpaceHandler.h"

#include "eckit/filesystem/FileSpaceStrategies.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/thread/AutoLock.h"

#include "fdb5/toc/FileSpace.h"
#include "fdb5/database/Key.h"

using eckit::Log;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static eckit::Mutex* local_mutex;
typedef std::map<std::string, FileSpaceHandlerInstance*> HandlerMap;
static HandlerMap* m = 0;
static pthread_once_t once = PTHREAD_ONCE_INIT;
static void init() {
    local_mutex = new eckit::Mutex();
    m = new HandlerMap();
}

//----------------------------------------------------------------------------------------------------------------------

const FileSpaceHandler& FileSpaceHandlerInstance::get()
{
    if(!instance_) { instance_ = make(); }
    return *instance_ ;
}

FileSpaceHandlerInstance::FileSpaceHandlerInstance(const std::string& name) : name_(name) {
    FileSpaceHandler::regist(name_, this);
}

FileSpaceHandlerInstance::~FileSpaceHandlerInstance() {
    FileSpaceHandler::unregist(name_);
}

const FileSpaceHandler& FileSpaceHandler::lookup(const std::string& name)
{
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    HandlerMap::const_iterator j = m->find(name);

    if (j == m->end()) {
        eckit::Log::error() << "No FileSpaceHandler factory for [" << name << "]" << std::endl;
        eckit::Log::error() << "Available FileSpaceHandler's are:" << std::endl;
        for (j = m->begin() ; j != m->end() ; ++j)
            eckit::Log::error() << "   " << (*j).first << std::endl;
        throw eckit::SeriousBug(std::string("No FileSpaceHandler called ") + name);
    }

    return (*j).second->get();
}

void FileSpaceHandler::regist(const std::string& name, FileSpaceHandlerInstance* h) {
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);
    HandlerMap::const_iterator j = m->find(name);
    ASSERT(j == m->end());
    (*m)[name] = h;
}

void FileSpaceHandler::unregist(const std::string& name) {
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);
    HandlerMap::const_iterator j = m->find(name);
    ASSERT(j != m->end());
    m->erase(name);
}

FileSpaceHandler::~FileSpaceHandler() {
}

FileSpaceHandler::FileSpaceHandler() {
}

//----------------------------------------------------------------------------------------------------------------------

namespace detail {

struct First : public FileSpaceHandler {
    First() {}
    eckit::PathName selectFileSystem(const Key& key, const FileSpace& fs) const {
        std::vector<eckit::PathName> roots = fs.writable();
        ASSERT(roots.size());
        return roots[0];
    }
};

FileSpaceHandlerRegister<First> def("Default");
FileSpaceHandlerRegister<First> first("First");

struct LeastUsed : public FileSpaceHandler {
    LeastUsed() {}
    eckit::PathName  selectFileSystem(const Key& key, const FileSpace& fs) const {
        return eckit::FileSpaceStrategies::leastUsed(fs.writable());
    }
};

FileSpaceHandlerRegister<LeastUsed> leastUsed("LeastUsed");

struct LeastUsedPercent : public FileSpaceHandler {
    LeastUsedPercent() {}
    eckit::PathName  selectFileSystem(const Key& key, const FileSpace& fs) const {
        return eckit::FileSpaceStrategies::leastUsedPercent(fs.writable());
    }
};

FileSpaceHandlerRegister<LeastUsedPercent> leastUsedPercent("LeastUsedPercent");

struct RoundRobin : public FileSpaceHandler {
    RoundRobin() {}
    eckit::PathName  selectFileSystem(const Key& key, const FileSpace& fs) const {
        return eckit::FileSpaceStrategies::roundRobin(fs.writable());
    }
};

FileSpaceHandlerRegister<RoundRobin> roundRobin("RoundRobin");

struct Random : public FileSpaceHandler {
    Random() {}
    eckit::PathName  selectFileSystem(const Key& key, const FileSpace& fs) const {
        return eckit::FileSpaceStrategies::pureRandom(fs.writable());
    }
};

FileSpaceHandlerRegister<Random> pureRandom("Random");

struct WeightedRandom : public FileSpaceHandler {
    WeightedRandom() {}
    eckit::PathName  selectFileSystem(const Key& key, const FileSpace& fs) const {
        return eckit::FileSpaceStrategies::weightedRandom(fs.writable());
    }
};

FileSpaceHandlerRegister<WeightedRandom> weightedRandom("WeightedRandom");

struct WeightedRandomPercent : public FileSpaceHandler {
    WeightedRandomPercent() {}
    eckit::PathName  selectFileSystem(const Key& key, const FileSpace& fs) const {
        return eckit::FileSpaceStrategies::weightedRandomPercent(fs.writable());
    }
};

FileSpaceHandlerRegister<WeightedRandomPercent> weightedRandomPercent("WeightedRandomPercent");

}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

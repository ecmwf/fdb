/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <sstream>

#include "fdb5/toc/FileSpaceHandler.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/FileSpaceStrategies.h"
#include "eckit/thread/AutoLock.h"

#include "fdb5/database/Key.h"
#include "fdb5/toc/FileSpace.h"

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

const FileSpaceHandler& FileSpaceHandlerInstance::get(const Config& config) {
    if (!instance_) {
        instance_ = make(config);
    }
    return *instance_;
}

FileSpaceHandlerInstance::FileSpaceHandlerInstance(const std::string& name) : name_(name) {
    FileSpaceHandler::regist(name_, this);
}

FileSpaceHandlerInstance::~FileSpaceHandlerInstance() {
    FileSpaceHandler::unregist(name_);
}

const FileSpaceHandler& FileSpaceHandler::lookup(const std::string& name, const Config& config) {
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    HandlerMap::const_iterator j = m->find(name);

    if (j == m->end()) {
        eckit::Log::error() << "No FileSpaceHandler factory for [" << name << "]" << std::endl;
        eckit::Log::error() << "Available FileSpaceHandler's are:" << std::endl;
        for (j = m->begin(); j != m->end(); ++j) {
            eckit::Log::error() << "   " << (*j).first << std::endl;
        }
        throw eckit::SeriousBug(std::string("No FileSpaceHandler called ") + name);
    }

    return (*j).second->get(config);
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

FileSpaceHandler::~FileSpaceHandler() {}

FileSpaceHandler::FileSpaceHandler(const Config& config) : config_(config) {}

//----------------------------------------------------------------------------------------------------------------------

namespace detail {

struct First : public FileSpaceHandler {
    First(const Config& config) : FileSpaceHandler(config) {}
    eckit::PathName selectFileSystem(const Key&, const FileSpace& fs) const {
        std::vector<eckit::PathName> roots = fs.enabled(ControlIdentifier::Archive);
        if (not roots.size()) {
            std::ostringstream oss;
            oss << "No writable roots available. Configured roots: " << fs.roots();
            throw eckit::UnexpectedState(oss.str(), Here());
        }
        return roots[0];
    }
};

static FileSpaceHandlerRegister<First> def("Default");
static FileSpaceHandlerRegister<First> first("First");

struct LeastUsed : public FileSpaceHandler {
    LeastUsed(const Config& config) : FileSpaceHandler(config) {}
    eckit::PathName selectFileSystem(const Key&, const FileSpace& fs) const {
        return eckit::FileSpaceStrategies::leastUsed(fs.enabled(ControlIdentifier::Archive));
    }
};

static FileSpaceHandlerRegister<LeastUsed> leastUsed("LeastUsed");

struct LeastUsedPercent : public FileSpaceHandler {
    LeastUsedPercent(const Config& config) : FileSpaceHandler(config) {}
    eckit::PathName selectFileSystem(const Key&, const FileSpace& fs) const {
        return eckit::FileSpaceStrategies::leastUsedPercent(fs.enabled(ControlIdentifier::Archive));
    }
};

static FileSpaceHandlerRegister<LeastUsedPercent> leastUsedPercent("LeastUsedPercent");

struct RoundRobin : public FileSpaceHandler {
    RoundRobin(const Config& config) : FileSpaceHandler(config) {}
    eckit::PathName selectFileSystem(const Key&, const FileSpace& fs) const {
        return eckit::FileSpaceStrategies::roundRobin(fs.enabled(ControlIdentifier::Archive));
    }
};

static FileSpaceHandlerRegister<RoundRobin> roundRobin("RoundRobin");

struct Random : public FileSpaceHandler {
    Random(const Config& config) : FileSpaceHandler(config) {}
    eckit::PathName selectFileSystem(const Key&, const FileSpace& fs) const {
        return eckit::FileSpaceStrategies::pureRandom(fs.enabled(ControlIdentifier::Archive));
    }
};

static FileSpaceHandlerRegister<Random> pureRandom("PureRandom"); /* alias to match name in MARS server */
static FileSpaceHandlerRegister<Random> random("Random");

struct WeightedRandom : public FileSpaceHandler {
    WeightedRandom(const Config& config) : FileSpaceHandler(config) {}
    eckit::PathName selectFileSystem(const Key&, const FileSpace& fs) const {
        return eckit::FileSpaceStrategies::weightedRandom(fs.enabled(ControlIdentifier::Archive));
    }
};

static FileSpaceHandlerRegister<WeightedRandom> weightedRandom("WeightedRandom");

struct WeightedRandomPercent : public FileSpaceHandler {
    WeightedRandomPercent(const Config& config) : FileSpaceHandler(config) {}
    eckit::PathName selectFileSystem(const Key&, const FileSpace& fs) const {
        return eckit::FileSpaceStrategies::weightedRandomPercent(fs.enabled(ControlIdentifier::Archive));
    }
};

static FileSpaceHandlerRegister<WeightedRandomPercent> weightedRandomPercent("WeightedRandomPercent");

}  // namespace detail

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

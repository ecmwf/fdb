/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <map>

#include "fdb5/database/Store.h"

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/Mutex.h"
#include "eckit/utils/StringTools.h"

#include "fdb5/LibFdb5.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

void Store::archiveCb(const Key& key, const void* data, eckit::Length length,
                      std::function<void(const std::unique_ptr<const FieldLocation> fieldLocation)> catalogue_archive) {
    catalogue_archive(archive(key, data, length));
}

std::unique_ptr<const FieldLocation> Store::archive(const Key& /*key*/, const void* /*data*/,
                                                    eckit::Length /*length*/) {
    NOTIMP;
}

bool Store::canMoveTo(const Key& /*key*/, const Config& /*config*/, const eckit::URI& /*dest*/) const {
    std::ostringstream ss;
    ss << "Store type " << type() << " does not support move" << std::endl;
    throw eckit::UserError(ss.str(), Here());
}

//----------------------------------------------------------------------------------------------------------------------

StoreFactory::StoreFactory() {}

StoreFactory& StoreFactory::instance() {
    static StoreFactory theOne;
    return theOne;
}

void StoreFactory::add(const std::string& name, StoreBuilderBase* builder) {
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    if (has(nameLowercase)) {
        throw eckit::SeriousBug("Duplicate entry in StoreFactory: " + nameLowercase, Here());
    }
    builders_[nameLowercase] = builder;
}

void StoreFactory::remove(const std::string& name) {
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    builders_.erase(nameLowercase);
}

bool StoreFactory::has(const std::string& name) {
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    return builders_.find(nameLowercase) != builders_.end();
}

void StoreFactory::list(std::ostream& out) {
    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    const char* sep = "";
    for (std::map<std::string, StoreBuilderBase*>::const_iterator j = builders_.begin(); j != builders_.end(); ++j) {
        out << sep << (*j).first;
        sep = ", ";
    }
}

std::unique_ptr<Store> StoreFactory::build(const Key& key, const Config& config) {
    std::string name          = config.getString("store", "file");
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    auto j = builders_.find(nameLowercase);

    LOG_DEBUG_LIB(LibFdb5) << "Looking for StoreBuilder [" << nameLowercase << "]" << std::endl;

    if (j == builders_.end()) {
        eckit::Log::error() << "No StoreBuilder for [" << nameLowercase << "]" << std::endl;
        eckit::Log::error() << "StoreBuilders are:" << std::endl;
        for (j = builders_.begin(); j != builders_.end(); ++j)
            eckit::Log::error() << "   " << (*j).first << std::endl;
        throw eckit::SeriousBug(std::string("No StoreBuilder called ") + nameLowercase);
    }

    return (*j).second->make(key, config);
}

//----------------------------------------------------------------------------------------------------------------------

StoreBuilderBase::StoreBuilderBase(const std::string& name) : name_(name) {
    StoreFactory::instance().add(name_, this);
}

StoreBuilderBase::~StoreBuilderBase() {
    if (LibFdb5::instance().dontDeregisterFactories())
        return;
    StoreFactory::instance().remove(name_);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

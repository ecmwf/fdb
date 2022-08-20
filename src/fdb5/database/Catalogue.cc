/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cstring>
#include <map>

#include "fdb5/database/Catalogue.h"

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/Mutex.h"
#include "eckit/utils/StringTools.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Manager.h"

namespace fdb5 {

std::unique_ptr<Store> Catalogue::buildStore(const Config& config) {
    if (buildByKey_)
        return StoreFactory::instance().build(schema(), key(), config);
    else {
        std::string name = config.getString("store", "file");

        return StoreFactory::instance().build(schema(), eckit::URI(name, uri()), config);
    }
}

bool Catalogue::enabled(const ControlIdentifier& controlIdentifier) const {
    return controlIdentifiers_.enabled(controlIdentifier);
}

std::ostream &operator<<(std::ostream &s, const Catalogue &x) {
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

CatalogueFactory::CatalogueFactory() {}

CatalogueFactory& CatalogueFactory::instance() {
    static CatalogueFactory theOne;
    return theOne;
}

void CatalogueFactory::add(const std::string& name, CatalogueBuilderBase* builder) {
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    if (has(nameLowercase)) {
        throw eckit::SeriousBug("Duplicate entry in CatalogueFactory: " + nameLowercase, Here());
    }
    builders_[nameLowercase] = builder;
}

void CatalogueFactory::remove(const std::string& name) {
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    builders_.erase(nameLowercase);
}

bool CatalogueFactory::has(const std::string& name) {
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    return builders_.find(nameLowercase) != builders_.end();
}

void CatalogueFactory::list(std::ostream& out) {
    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    const char* sep = "";
    for (std::map<std::string, CatalogueBuilderBase*>::const_iterator j = builders_.begin(); j != builders_.end(); ++j) {
        out << sep << (*j).first;
        sep = ", ";
    }
}

std::unique_ptr<Catalogue> CatalogueFactory::build(const Key& key, const Config& config, bool read) {
    std::string name = Manager(config).engine(key);
    std::string nameLowercase = eckit::StringTools::lower(name);

    nameLowercase += read ? ".reader" : ".writer";

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    auto j = builders_.find(nameLowercase);

    eckit::Log::debug() << "Looking for CatalogueBuilder [" << nameLowercase << "]" << std::endl;

    if (j == builders_.end()) {
        eckit::Log::error() << "No CatalogueBuilder for [" << nameLowercase << "]" << std::endl;
        eckit::Log::error() << "CatalogueBuilders are:" << std::endl;
        for (j = builders_.begin(); j != builders_.end(); ++j)
            eckit::Log::error() << "   " << (*j).first << std::endl;
        throw eckit::SeriousBug(std::string("No CatalogueBuilder called ") + nameLowercase);
    }

    return j->second->make(key, config);
}

std::unique_ptr<Catalogue> CatalogueFactory::build(const eckit::URI& uri, const fdb5::Config& config, bool read) {
    std::string name = uri.scheme();
    std::string nameLowercase = eckit::StringTools::lower(name);

    nameLowercase += read ? ".reader" : ".writer";

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    auto j = builders_.find(nameLowercase);

    eckit::Log::debug() << "Looking for CatalogueBuilder [" << nameLowercase << "]" << std::endl;

    if (j == builders_.end()) {
        eckit::Log::error() << "No CatalogueBuilder for [" << nameLowercase << "]" << std::endl;
        eckit::Log::error() << "CatalogueBuilders are:" << std::endl;
        for (j = builders_.begin(); j != builders_.end(); ++j)
            eckit::Log::error() << "   " << (*j).first << std::endl;
        throw eckit::SeriousBug(std::string("No CatalogueBuilder called ") + nameLowercase);
    }

    return j->second->make(uri, config);
}

//----------------------------------------------------------------------------------------------------------------------

CatalogueBuilderBase::CatalogueBuilderBase(const std::string& name) : name_(name) {
    CatalogueFactory::instance().add(name_, this);
}

CatalogueBuilderBase::~CatalogueBuilderBase() {
    if(LibFdb5::instance().dontDeregisterFactories()) return;
    CatalogueFactory::instance().remove(name_);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

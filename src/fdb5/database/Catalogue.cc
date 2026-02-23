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

#include "eckit/exception/Exceptions.h"
#include "eckit/io/AutoCloser.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/Mutex.h"
#include "eckit/utils/StringTools.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Manager.h"
#include "fdb5/database/Store.h"

namespace fdb5 {

std::ostream& operator<<(std::ostream& s, const Catalogue& x) {
    x.print(s);
    return s;
}

std::unique_ptr<Store> CatalogueImpl::buildStore() const {
    return StoreFactory::instance().build(key(), config_);
}

void Catalogue::visitEntries(EntryVisitor& visitor, bool sorted) {

    auto all = indexes(sorted);  // Deferred reading indexes.

    // It is likely that many indexes in the same database share resources/files/etc.
    // To prevent repeated opening/closing (especially where a PooledFile would facilitate things)
    // pre-open the indexes, and keep them open
    std::vector<eckit::AutoCloser<Index>> closers;
    closers.reserve(all.size());

    // Allow the visitor to selectively reject this DB.
    if (visitor.visitDatabase(*this)) {
        if (visitor.visitIndexes()) {
            for (Index& idx : all) {
                if (visitor.visitEntries()) {
                    closers.emplace_back(idx);
                    idx.entries(visitor);  // contains visitIndex
                }
                else {
                    visitor.visitIndex(idx);
                }
            }
        }
    }

    visitor.catalogueComplete(*this);
}

const Key CatalogueWriter::currentIndexKey() {
    return currentIndex().key();
}

bool CatalogueImpl::enabled(const ControlIdentifier& controlIdentifier) const {
    return controlIdentifiers_.enabled(controlIdentifier);
}

void CatalogueReader::invalidateAxis() {
    axisCache_.clear();
}

std::optional<std::reference_wrapper<const Axis>> CatalogueReader::axis(const std::string& keyword) const {
    if (const auto iter = axisCache_.find(keyword); iter != std::end(axisCache_)) {
        return iter->second;
    }

    std::optional<Axis> newAxis{computeAxis(keyword)};
    if (!newAxis) {
        return std::nullopt;
    }

    const auto [it, success] = axisCache_.emplace(keyword, std::move(newAxis.value()));
    ASSERT(success);

    return it->second;
}

//----------------------------------------------------------------------------------------------------------------------

CatalogueReaderFactory::CatalogueReaderFactory() {}

CatalogueReaderFactory& CatalogueReaderFactory::instance() {
    static CatalogueReaderFactory theOne;
    return theOne;
}

void CatalogueReaderFactory::add(const std::string& name, CatalogueReaderBuilderBase* builder) {
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    if (has(nameLowercase)) {
        throw eckit::SeriousBug("Duplicate entry in CatalogueReaderFactory: " + nameLowercase, Here());
    }
    builders_[nameLowercase] = builder;
}

void CatalogueReaderFactory::remove(const std::string& name) {
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    builders_.erase(nameLowercase);
}

bool CatalogueReaderFactory::has(const std::string& name) {
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    return builders_.find(nameLowercase) != builders_.end();
}

void CatalogueReaderFactory::list(std::ostream& out) {
    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    const char* sep = "";
    for (std::map<std::string, CatalogueReaderBuilderBase*>::const_iterator j = builders_.begin(); j != builders_.end();
         ++j) {
        out << sep << (*j).first;
        sep = ", ";
    }
}

std::unique_ptr<CatalogueReader> CatalogueReaderFactory::build(const Key& dbKey, const Config& config) {
    std::string name          = Manager(config).engine(dbKey);
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    auto j = builders_.find(nameLowercase);

    LOG_DEBUG_LIB(LibFdb5) << "Looking for CatalogueReaderBuilder [" << nameLowercase << "]" << std::endl;

    if (j == builders_.end()) {
        eckit::Log::error() << "No CatalogueReaderBuilder for [" << nameLowercase << "]" << std::endl;
        eckit::Log::error() << "CatalogueReaderBuilders are:" << std::endl;
        for (j = builders_.begin(); j != builders_.end(); ++j)
            eckit::Log::error() << "   " << (*j).first << std::endl;
        throw eckit::SeriousBug(std::string("No CatalogueReaderBuilder called ") + nameLowercase);
    }

    return j->second->make(dbKey, config);
}

std::unique_ptr<CatalogueReader> CatalogueReaderFactory::build(const eckit::URI& uri, const fdb5::Config& config) {
    std::string name          = uri.scheme();
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    auto j = builders_.find(nameLowercase);

    LOG_DEBUG_LIB(LibFdb5) << "Looking for CatalogueReaderBuilder [" << nameLowercase << "]" << std::endl;

    if (j == builders_.end()) {
        eckit::Log::error() << "No CatalogueReaderBuilder for [" << nameLowercase << "]" << std::endl;
        eckit::Log::error() << "CatalogueReaderBuilders are:" << std::endl;
        for (j = builders_.begin(); j != builders_.end(); ++j)
            eckit::Log::error() << "   " << (*j).first << std::endl;
        throw eckit::SeriousBug(std::string("No CatalogueReaderBuilder called ") + nameLowercase);
    }

    return j->second->make(uri, config);
}

//----------------------------------------------------------------------------------------------------------------------

CatalogueReaderBuilderBase::CatalogueReaderBuilderBase(const std::string& name) : name_(name) {
    CatalogueReaderFactory::instance().add(name_, this);
}

CatalogueReaderBuilderBase::~CatalogueReaderBuilderBase() {
    if (LibFdb5::instance().dontDeregisterFactories())
        return;
    CatalogueReaderFactory::instance().remove(name_);
}


//----------------------------------------------------------------------------------------------------------------------

CatalogueWriterFactory::CatalogueWriterFactory() {}

CatalogueWriterFactory& CatalogueWriterFactory::instance() {
    static CatalogueWriterFactory theOne;
    return theOne;
}

void CatalogueWriterFactory::add(const std::string& name, CatalogueWriterBuilderBase* builder) {
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    if (has(nameLowercase)) {
        throw eckit::SeriousBug("Duplicate entry in CatalogueWriterFactory: " + nameLowercase, Here());
    }
    builders_[nameLowercase] = builder;
}

void CatalogueWriterFactory::remove(const std::string& name) {
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    builders_.erase(nameLowercase);
}

bool CatalogueWriterFactory::has(const std::string& name) {
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    return builders_.find(nameLowercase) != builders_.end();
}

void CatalogueWriterFactory::list(std::ostream& out) {
    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    const char* sep = "";
    for (std::map<std::string, CatalogueWriterBuilderBase*>::const_iterator j = builders_.begin(); j != builders_.end();
         ++j) {
        out << sep << (*j).first;
        sep = ", ";
    }
}

std::unique_ptr<CatalogueWriter> CatalogueWriterFactory::build(const Key& dbKey, const Config& config) {
    std::string name          = Manager(config).engine(dbKey);
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    auto j = builders_.find(nameLowercase);

    LOG_DEBUG_LIB(LibFdb5) << "Looking for CatalogueWriterBuilder [" << nameLowercase << "]" << std::endl;

    if (j == builders_.end()) {
        eckit::Log::error() << "No CatalogueWriterBuilder for [" << nameLowercase << "]" << std::endl;
        eckit::Log::error() << "CatalogueWriterBuilders are:" << std::endl;
        for (j = builders_.begin(); j != builders_.end(); ++j)
            eckit::Log::error() << "   " << (*j).first << std::endl;
        throw eckit::SeriousBug(std::string("No CatalogueWriterBuilder called ") + nameLowercase);
    }

    return j->second->make(dbKey, config);
}

std::unique_ptr<CatalogueWriter> CatalogueWriterFactory::build(const eckit::URI& uri, const fdb5::Config& config) {
    std::string name          = uri.scheme();
    std::string nameLowercase = eckit::StringTools::lower(name);

    eckit::AutoLock<eckit::Mutex> lock(mutex_);
    auto j = builders_.find(nameLowercase);

    LOG_DEBUG_LIB(LibFdb5) << "Looking for CatalogueWriterBuilder [" << nameLowercase << "]" << std::endl;

    if (j == builders_.end()) {
        eckit::Log::error() << "No CatalogueWriterBuilder for [" << nameLowercase << "]" << std::endl;
        eckit::Log::error() << "CatalogueWriterBuilders are:" << std::endl;
        for (j = builders_.begin(); j != builders_.end(); ++j)
            eckit::Log::error() << "   " << (*j).first << std::endl;
        throw eckit::SeriousBug(std::string("No CatalogueWriterBuilder called ") + nameLowercase);
    }

    return j->second->make(uri, config);
}

//----------------------------------------------------------------------------------------------------------------------

CatalogueWriterBuilderBase::CatalogueWriterBuilderBase(const std::string& name) : name_(name) {
    CatalogueWriterFactory::instance().add(name_, this);
}

CatalogueWriterBuilderBase::~CatalogueWriterBuilderBase() {
    if (LibFdb5::instance().dontDeregisterFactories())
        return;
    CatalogueWriterFactory::instance().remove(name_);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

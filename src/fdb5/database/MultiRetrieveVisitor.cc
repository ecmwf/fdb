/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/MultiRetrieveVisitor.h"

#include <memory>

#include "eckit/config/Resource.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Key.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/types/Type.h"
#include "fdb5/types/TypesRegistry.h"



namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

MultiRetrieveVisitor::MultiRetrieveVisitor(const Notifier& wind,
                                           InspectIterator& iterator,
                                           eckit::CacheLRU<Key,CatalogueReader*>& databases,
                                           const Config& config) :
    wind_(wind),
    databases_(databases),
    iterator_(iterator),
    config_(config) {
}

MultiRetrieveVisitor::~MultiRetrieveVisitor() {
}

// From Visitor

bool MultiRetrieveVisitor::selectDatabase(const Key& dbKey, const TypedKey& fullComputedKey) {

	LOG_DEBUG_LIB(LibFdb5) << "FDB5 selectDatabase " << dbKey  << std::endl;

    /* is it the current DB ? */

    if(catalogue_) {
        if(dbKey == catalogue_->key()) {
            eckit::Log::info() << "This is the current db" << std::endl;
            return true;
        }
    }

    /* is the DB already open ? */

    if(databases_.exists(dbKey)) {
        LOG_DEBUG_LIB(LibFdb5) << "FDB5 Reusing database " << dbKey << std::endl;
        catalogue_ = databases_.access(dbKey);
        return true;
    }

    /* DB not yet open */

    std::unique_ptr<CatalogueReader> newCatalogue = CatalogueReaderFactory::instance().build(dbKey, config_);

    // If this database is locked for retrieval then it "does not exist"
    if (!newCatalogue->enabled(ControlIdentifier::Retrieve)) {
        std::ostringstream ss;
        ss << "Catalogue " << *newCatalogue << " is LOCKED for retrieval";
        eckit::Log::warning() << ss.str() << std::endl;
        return false;
    }

    LOG_DEBUG_LIB(LibFdb5) << "MultiRetrieveVisitor::selectDatabase opening database " << dbKey << " (type=" << newCatalogue->type() << ")" << std::endl;

    if (!newCatalogue->open()) {
        LOG_DEBUG_LIB(LibFdb5) << "Database does not exist " << dbKey << std::endl;
        return false;
    } else {
        catalogue_ = newCatalogue.release();
        databases_.insert(dbKey, catalogue_);
        return true;
    }
}

bool MultiRetrieveVisitor::selectIndex(const Key& idxKey, const TypedKey&) {
    ASSERT(catalogue_);
    LOG_DEBUG_LIB(LibFdb5) << "selectIndex " << idxKey << std::endl;
    return catalogue_->selectIndex(idxKey);
}

bool MultiRetrieveVisitor::selectDatum(const TypedKey& datumKey, const TypedKey& full) {
    ASSERT(catalogue_);
    LOG_DEBUG_LIB(LibFdb5) << "selectDatum " << datumKey << ", " << full << std::endl;

    Field field;
    if (catalogue_->retrieve(datumKey.canonical(), field)) {

        Key simplifiedKey;
        for (auto k = datumKey.begin(); k != datumKey.end(); k++) {
            if (!k->second.empty())
                simplifiedKey.set(k->first, k->second);
        }

        iterator_.emplace(ListElement({catalogue_->key(), catalogue_->indexKey(), simplifiedKey}, field.stableLocation(), field.timestamp()));
        return true;
    }

    return false;
}

void MultiRetrieveVisitor::values(const metkit::mars::MarsRequest &request,
                             const std::string &keyword,
                             const TypesRegistry &registry,
                             eckit::StringList &values) {
    eckit::StringList list;
    registry.lookupType(keyword).getValues(request, keyword, list, wind_, catalogue_);

    eckit::StringSet filter;
    bool toFilter = false;
    if (catalogue_) {
        toFilter = catalogue_->axis(keyword, filter);
    }

    for(const auto& l: list) {
        std::string v = registry.lookupType(keyword).toKey(l);
        if (!toFilter || filter.find(v) != filter.end()) {
            values.push_back(l);
        }
    }
}

void MultiRetrieveVisitor::print( std::ostream &out ) const {
    out << "MultiRetrieveVisitor[]";
}

const Schema& MultiRetrieveVisitor::databaseSchema() const {
    ASSERT(catalogue_);
    return catalogue_->schema();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

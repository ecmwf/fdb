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
#include "fdb5/database/DB.h"
#include "fdb5/database/Key.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/types/Type.h"
#include "fdb5/types/TypesRegistry.h"



namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

MultiRetrieveVisitor::MultiRetrieveVisitor(const Notifier& wind,
                                           InspectIterator& iterator,
                                           eckit::CacheLRU<Key,Catalogue*>& databases,
                                           const Config& config) :
    wind_(wind),
    databases_(databases),
    iterator_(iterator),
    config_(config) {
}

MultiRetrieveVisitor::~MultiRetrieveVisitor() {
}

// From Visitor

bool MultiRetrieveVisitor::selectDatabase(const Key& key, const Key&) {

	eckit::Log::debug() << "FDB5 selectDatabase " << key  << std::endl;

    /* is it the current DB ? */

    if(catalogue_) {
        if(key == catalogue_->key()) {
            eckit::Log::info() << "This is the current db" << std::endl;
            return true;
        }
    }

    /* is the DB already open ? */

    if(databases_.exists(key)) {
        eckit::Log::debug<LibFdb5>() << "FDB5 Reusing database " << key << std::endl;
        catalogue_ = databases_.access(key);
        return true;
    }

    /* DB not yet open */

    std::unique_ptr<Catalogue> newCatalogue = CatalogueFactory::instance().build(key, config_, true);

    // If this database is locked for retrieval then it "does not exist"
    if (!newCatalogue->enabled(ControlIdentifier::Retrieve)) {
        std::ostringstream ss;
        ss << "Catalogue " << *newCatalogue << " is LOCKED for retrieval";
        eckit::Log::warning() << ss.str() << std::endl;
        return false;
    }

    eckit::Log::debug<LibFdb5>() << "selectDatabase opening database " << key << " (type=" << newCatalogue->type() << ")" << std::endl;

    if (!newCatalogue->open()) {
        eckit::Log::debug() << "Database does not exist " << key << std::endl;
        return false;
    } else {
        catalogue_ = newCatalogue.release();
        databases_.insert(key, catalogue_);
        return true;
    }
}

bool MultiRetrieveVisitor::selectIndex(const Key& key, const Key&) {
    ASSERT(catalogue_);
    eckit::Log::debug() << "selectIndex " << key << std::endl;
    return catalogue_->selectIndex(key);
}

bool MultiRetrieveVisitor::selectDatum(const InspectionKey& key, const Key& full) {
    ASSERT(catalogue_);
    eckit::Log::debug() << "selectDatum " << key << ", " << full << std::endl;

    Field field;
    if (reader()->retrieve(key, field)) {

        Key simplifiedKey;
        for (auto k = key.begin(); k != key.end(); k++) {
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
    registry.lookupType(keyword).getValues(request, keyword, list, wind_, reader());

    eckit::StringSet filter;
    bool toFilter = false;
    if (catalogue_) {
        toFilter = reader()->axis(keyword, filter);
    }

    for(auto l: list) {
        std::string v = registry.lookupType(keyword).toKey(keyword, l);
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

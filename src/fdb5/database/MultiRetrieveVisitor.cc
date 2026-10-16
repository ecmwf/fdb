// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/database/MultiRetrieveVisitor.h"

#include <memory>
#include <ostream>
#include <sstream>

#include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Key.h"
#include "fdb5/types/Type.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

MultiRetrieveVisitor::MultiRetrieveVisitor(InspectIterator& iterator, eckit::CacheLRU<Key, CatalogueReader*>& databases,
                                           const Config& config) :
    databases_(databases), iterator_(iterator), config_(config) {}

MultiRetrieveVisitor::~MultiRetrieveVisitor() {}

// From Visitor

bool MultiRetrieveVisitor::selectDatabase(const Key& dbKey, const Key& /* fullKey */) {

    LOG_DEBUG_LIB(LibFdb5) << "FDB5 selectDatabase " << dbKey << std::endl;

    /* is it the current DB ? */

    if (catalogue_) {
        if (dbKey == catalogue_->key()) {
            eckit::Log::info() << "This is the current db" << std::endl;
            return true;
        }
    }

    // If the catalogue is being changed, we need to make sure any access has been recorded
    markAccessed();

    /* is the DB already open ? */

    if (databases_.exists(dbKey)) {
        LOG_DEBUG_LIB(LibFdb5) << "FDB5 Reusing database " << dbKey << std::endl;
        catalogue_ = databases_.access(dbKey);
        return true;
    }

    /* DB not yet open */

    auto newCatalogue = CatalogueReaderFactory::instance().build(dbKey, config_);

    // If this database is locked for retrieval then it "does not exist"
    if (!newCatalogue->enabled(ControlIdentifier::Retrieve)) {
        std::ostringstream ss;
        ss << "Catalogue " << *newCatalogue << " is LOCKED for retrieval";
        eckit::Log::warning() << ss.str() << std::endl;
        return false;
    }

    LOG_DEBUG_LIB(LibFdb5) << "MultiRetrieveVisitor::selectDatabase opening database " << dbKey
                           << " (type=" << newCatalogue->type() << ")" << std::endl;

    if (!newCatalogue->open()) {
        LOG_DEBUG_LIB(LibFdb5) << "Database does not exist " << dbKey << std::endl;
        return false;
    }
    else {
        catalogue_ = newCatalogue.release();
        databases_.insert(dbKey, catalogue_);
        return true;
    }
}

bool MultiRetrieveVisitor::selectIndex(const Key& idxKey) {
    ASSERT(catalogue_);
    LOG_DEBUG_LIB(LibFdb5) << "selectIndex " << idxKey << std::endl;
    return catalogue_->selectIndex(idxKey);
}

bool MultiRetrieveVisitor::selectDatum(const Key& datumKey, const Key& fullKey) {
    ASSERT(catalogue_);
    LOG_DEBUG_LIB(LibFdb5) << "selectDatum " << datumKey << ", " << fullKey << std::endl;

    Field field;
    if (catalogue_->retrieve(datumKey, field)) {

        Key simplifiedKey;
        for (const auto& [keyword, value] : datumKey) {
            if (!value.empty()) {
                simplifiedKey.push(keyword, value);
            }
        }

        iterator_.emplace(
            {catalogue_->key(), catalogue_->indexKey(), simplifiedKey, field.stableLocation(), field.timestamp()});
        accessed_ = true;
        return true;
    }

    return false;
}

void MultiRetrieveVisitor::markAccessed() {
    if (accessed_ && catalogue_) {
        catalogue_->markRead();
    }
    accessed_ = false;
}

void MultiRetrieveVisitor::deselectDatabase() {
    markAccessed();
    catalogue_ = nullptr;
}

void MultiRetrieveVisitor::print(std::ostream& out) const {
    out << "MultiRetrieveVisitor[]";
}

const Schema& MultiRetrieveVisitor::databaseSchema() const {
    ASSERT(catalogue_);
    return catalogue_->schema();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

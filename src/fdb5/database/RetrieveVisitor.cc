/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/RetrieveVisitor.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/types/Type.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RetrieveVisitor::RetrieveVisitor(const Notifier& wind, HandleGatherer& gatherer) :
    ReadVisitor(wind), gatherer_(gatherer) {}

// From Visitor

bool RetrieveVisitor::selectDatabase(const Key& dbKey, const Key& /*fullKey*/) {

    if (catalogue_) {
        if (dbKey == catalogue_->key()) {
            return true;
        }
    }

    LOG_DEBUG_LIB(LibFdb5) << "RetrieveVisitor::selectDatabase " << dbKey << std::endl;
    catalogue_ = CatalogueReaderFactory::instance().build(dbKey, fdb5::Config()).get();

    // If this database is locked for retrieval then it "does not exist"
    if (!catalogue_->enabled(ControlIdentifier::Retrieve)) {
        std::ostringstream ss;
        ss << "Database " << *catalogue_ << " is LOCKED for retrieval";
        eckit::Log::warning() << ss.str() << std::endl;
        catalogue_ = nullptr;
        return false;
    }

    if (!catalogue_->open()) {
        eckit::Log::info() << "Database does not exists " << dbKey << std::endl;
        return false;
    }

    return true;
}

bool RetrieveVisitor::selectIndex(const Key& idxKey) {
    ASSERT(catalogue_);
    return catalogue_->selectIndex(idxKey);
}

bool RetrieveVisitor::selectDatum(const Key& datumKey, const Key& /*fullKey*/) {
    ASSERT(catalogue_);

    Field field;
    eckit::DataHandle* dh = nullptr;
    if (catalogue_->retrieve(datumKey, field)) {
        dh = store().retrieve(field);
    }

    if (dh) {
        gatherer_.add(dh);
    }

    return (dh != 0);
}

void RetrieveVisitor::deselectDatabase() {
    catalogue_ = nullptr;
    return;
}

Store& RetrieveVisitor::store() {
    if (!store_) {
        ASSERT(catalogue_);
        store_ = catalogue_->buildStore();
        ASSERT(store_);
    }
    return *store_;
}

void RetrieveVisitor::print(std::ostream& out) const {
    out << "RetrieveVisitor[]";
}

const Schema& RetrieveVisitor::databaseSchema() const {
    ASSERT(catalogue_);
    return catalogue_->schema();
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

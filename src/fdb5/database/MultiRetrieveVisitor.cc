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
                                           HandleGatherer& gatherer,
                                           eckit::CacheLRU<Key,DB*>& databases,
                                           const Config& config) :
    db_(0),
    wind_(wind),
    databases_(databases),
    gatherer_(gatherer),
    config_(config) {
}

MultiRetrieveVisitor::~MultiRetrieveVisitor() {
}

// From Visitor

bool MultiRetrieveVisitor::selectDatabase(const Key& key, const Key&) {

	eckit::Log::debug() << "FDB5 selectDatabase " << key  << std::endl;

    /* is it the current DB ? */

    if(db_) {
        if(key == db_->key()) {
            eckit::Log::info() << "This is the current db" << std::endl;
            return true;
        }
    }

    /* is the DB already open ? */

    if(databases_.exists(key)) {
        eckit::Log::debug<LibFdb5>() << "FDB5 Reusing database " << key << std::endl;
        db_ = databases_.access(key);
        return true;
    }

    /* DB not yet open */

    std::unique_ptr<DB> newDB( DBFactory::buildReader(key, config_) );

    eckit::Log::debug<LibFdb5>() << "selectDatabase opening database " << key << " (type=" << newDB->dbType() << ")" << std::endl;

    if (!newDB->open()) {
        eckit::Log::debug() << "Database does not exist " << key << std::endl;
        return false;
    } else {
        newDB->checkSchema(key);
        db_ = newDB.release();
        databases_.insert(key, db_);
        return true;
    }
}

bool MultiRetrieveVisitor::selectIndex(const Key& key, const Key&) {
    ASSERT(db_);
    eckit::Log::debug() << "selectIndex " << key << std::endl;
    return db_->selectIndex(key);
}

bool MultiRetrieveVisitor::selectDatum(const Key& key, const Key& full) {
    ASSERT(db_);
    eckit::Log::debug() << "selectDatum " << key << ", " << full << std::endl;

    eckit::DataHandle *dh = db_->retrieve(key);

    if (dh) {
        gatherer_.add(dh);
    }

    return (dh != 0);
}

void MultiRetrieveVisitor::values(const metkit::MarsRequest &request, const std::string &keyword,
                             const TypesRegistry &registry,
                             eckit::StringList &values) {
    registry.lookupType(keyword).getValues(request, keyword, values, wind_, db_);
}

void MultiRetrieveVisitor::print( std::ostream &out ) const {
    out << "MultiRetrieveVisitor[]";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

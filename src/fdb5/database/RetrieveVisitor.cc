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

#include "eckit/config/Resource.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/DB.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/types/Type.h"
#include "fdb5/types/TypesRegistry.h"



namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RetrieveVisitor::RetrieveVisitor(const Notifier &wind, HandleGatherer &gatherer):
    wind_(wind),
    gatherer_(gatherer) {
}

RetrieveVisitor::~RetrieveVisitor() {
}

// From Visitor

bool RetrieveVisitor::selectDatabase(const Key& key, const Key&) {

    if(db_) {
        if(key == db_->key()) {
            return true;
        }
    }

    eckit::Log::debug<LibFdb5>() << "selectDatabase " << key << std::endl;
//    db_.reset(DBFactory::buildReader(key));
    db_ = DB::buildReader(key);

    // If this database is locked for retrieval then it "does not exist"
    if (!db_->enabled(ControlIdentifier::Retrieve)) {
        std::ostringstream ss;
        ss << "Database " << *db_ << " is LOCKED for retrieval";
        eckit::Log::warning() << ss.str() << std::endl;
        db_.reset();
        return false;
    }

    if (!db_->open()) {
        eckit::Log::info() << "Database does not exists " << key << std::endl;
        return false;
    } else {
        return true;
    }
}

bool RetrieveVisitor::selectIndex(const Key& key, const Key&) {
    ASSERT(db_);
    // eckit::Log::info() << "selectIndex " << key << std::endl;
    return db_->selectIndex(key);
}

bool RetrieveVisitor::selectDatum(const Key& key, const Key&) {
    ASSERT(db_);
    // eckit::Log::info() << "selectDatum " << key << ", " << full << std::endl;

    eckit::DataHandle *dh = db_->retrieve(key);

    if (dh) {
        gatherer_.add(dh);
    }

    return (dh != 0);
}

void RetrieveVisitor::values(const metkit::mars::MarsRequest &request,
                             const std::string &keyword,
                             const TypesRegistry &registry,
                             eckit::StringList &values) {
    eckit::StringList list;
    registry.lookupType(keyword).getValues(request, keyword, list, wind_, db_.get());

    eckit::StringSet filter;
    bool toFilter = false;
    if (db_) {
        toFilter = db_->axis(keyword, filter);
    }

    for(auto l: list) {
        std::string v = registry.lookupType(keyword).toKey(keyword, l);
        if (!toFilter || filter.find(v) != filter.end()) {
            values.push_back(l);
        }
    }
}

void RetrieveVisitor::print( std::ostream &out ) const {
    out << "RetrieveVisitor[]";
}

const Schema& RetrieveVisitor::databaseSchema() const {
    ASSERT(db_);
    return db_->schema();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

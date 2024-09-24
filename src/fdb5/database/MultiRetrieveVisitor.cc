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
#include <ostream>
#include <sstream>
#include <string>

#include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/Key.h"
#include "fdb5/types/Type.h"
#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

MultiRetrieveVisitor::MultiRetrieveVisitor(const Notifier& wind,
                                           InspectIterator& iterator,
                                           eckit::CacheLRU<Key,DB*>& databases,
                                           const Config& config) :
    db_(nullptr),
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

    if(db_) {
        if(dbKey == db_->key()) {
            eckit::Log::info() << "This is the current db" << std::endl;
            return true;
        }
    }

    /* is the DB already open ? */

    if(databases_.exists(dbKey)) {
        LOG_DEBUG_LIB(LibFdb5) << "FDB5 Reusing database " << dbKey << std::endl;
        db_ = databases_.access(dbKey);
        return true;
    }

    /* DB not yet open */
    std::unique_ptr<DB> newDB = DB::buildReader(dbKey, config_);

    // If this database is locked for retrieval then it "does not exist"
    if (!newDB->enabled(ControlIdentifier::Retrieve)) {
        std::ostringstream ss;
        ss << "Database " << *newDB << " is LOCKED for retrieval";
        eckit::Log::warning() << ss.str() << std::endl;
        return false;
    }

    LOG_DEBUG_LIB(LibFdb5) << "MultiRetrieveVisitor::selectDatabase opening database " << dbKey << " (type=" << newDB->dbType() << ")" << std::endl;

    if (!newDB->open()) {
        LOG_DEBUG_LIB(LibFdb5) << "Database does not exist " << dbKey << std::endl;
        return false;
    } else {
        db_ = newDB.release();
        databases_.insert(dbKey, db_);
        return true;
    }
}

bool MultiRetrieveVisitor::selectIndex(const Key& idxKey, const TypedKey&) {
    ASSERT(db_);
    LOG_DEBUG_LIB(LibFdb5) << "selectIndex " << idxKey << std::endl;
    return db_->selectIndex(idxKey);
}

bool MultiRetrieveVisitor::selectDatum(const TypedKey& datumKey, const TypedKey& full) {
    ASSERT(db_);
    LOG_DEBUG_LIB(LibFdb5) << "selectDatum " << datumKey << ", " << full << std::endl;

    Field field;
    if (db_->inspect(datumKey.canonical(), field)) {

        Key simplifiedKey;
        for (auto k = datumKey.begin(); k != datumKey.end(); k++) {
            if (!k->second.empty())
                simplifiedKey.set(k->first, k->second);
        }

        iterator_.emplace({db_->key(), db_->indexKey(), simplifiedKey, field.stableLocation(), field.timestamp()});
        return true;
    }

    return false;
}

void MultiRetrieveVisitor::values(const metkit::mars::MarsRequest &request,
                             const std::string &keyword,
                             const TypesRegistry &registry,
                             eckit::StringList &values) {
    eckit::StringList list;
    registry.lookupType(keyword).getValues(request, keyword, list, wind_, db_);

    eckit::DenseSet<std::string> filter;
    bool toFilter = false;
    if (db_) {
        toFilter = db_->axis(keyword, filter);
    }

    for (const auto& value : list) {
        std::string v = registry.lookupType(keyword).toKey(value);
        if (!toFilter || filter.find(v) != filter.end()) {
            values.push_back(value);
        }
    }
}

void MultiRetrieveVisitor::print( std::ostream &out ) const {
    out << "MultiRetrieveVisitor[]";
}

const Schema& MultiRetrieveVisitor::databaseSchema() const {
    ASSERT(db_);
    return db_->schema();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

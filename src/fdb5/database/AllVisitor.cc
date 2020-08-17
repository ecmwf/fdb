/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/AllVisitor.h"

#include "eckit/config/Resource.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/DB.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/types/Type.h"
#include "fdb5/types/TypesRegistry.h"



namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

AllVisitor::AllVisitor(std::vector<Key>* keys, const Schema& schema):
    schema_(schema),
    keys_(keys) {
}

AllVisitor::~AllVisitor() {
}


bool AllVisitor::selectDatabase(const Key& key, const Key&) {

    k0_ = key;
    return true;
}

bool AllVisitor::selectIndex(const Key &key, const Key &full) {

    k1_ = key;
    return true;
}

bool AllVisitor::selectDatum(const Key& key, const Key&) {

    Key combined;
    for (const auto& kv : k0_) {
        combined.set(kv.first, kv.second);
    }
    for (const auto& kv : k1_) {
        combined.set(kv.first, kv.second);
    }
    for (const auto& kv : key) {
        combined.set(kv.first, kv.second);
    }

    keys_->push_back(combined);
    return true;
}

void AllVisitor::values(const metkit::mars::MarsRequest &request, const std::string &keyword,
                             const TypesRegistry &registry,
                             eckit::StringList &values) {
    values = request.values(keyword);
}

void AllVisitor::print( std::ostream &out ) const {
//    out << "AllVisitor[]";
}

const Schema& AllVisitor::databaseSchema() const {
    return schema_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

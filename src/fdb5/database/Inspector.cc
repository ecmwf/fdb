/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/Inspector.h"

#include <memory>

#include "eckit/config/Resource.h"
#include "eckit/log/Log.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/MultiRetrieveVisitor.h"
#include "fdb5/database/Notifier.h"
#include "fdb5/rules/Schema.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

InspectIterator::InspectIterator() : index_(0) {}

InspectIterator::~InspectIterator() {
    queue_.clear();
}

void InspectIterator::emplace(ListElement&& elem) {
    queue_.push_back(elem);
}

bool InspectIterator::next(ListElement& elem) {
    if (index_ >= queue_.size()) {
        return false;
    }
    elem = queue_[index_];
    index_++;
    return true;
}

//----------------------------------------------------------------------------------------------------------------------

static void purgeCatalogue(Key& key, CatalogueReader*& db) {
    LOG_DEBUG_LIB(LibFdb5) << "Purging DB with key " << key << std::endl;
    delete db;
}

Inspector::Inspector(const Config& dbConfig) :
    databases_(Resource<size_t>("fdbMaxOpenDatabases", 16), &purgeCatalogue), dbConfig_(dbConfig) {}

ListIterator Inspector::inspect(const metkit::mars::MarsRequest& request, const Schema& schema,
                                const fdb5::Notifier& notifyee) const {

    auto iterator = std::make_unique<InspectIterator>();
    MultiRetrieveVisitor visitor(notifyee, *iterator, databases_, dbConfig_);

    LOG_DEBUG_LIB(LibFdb5) << "Using schema: " << schema << std::endl;

    schema.expand(request, visitor);

    using QueryIterator = APIIterator<ListElement>;
    return QueryIterator(iterator.release());
}

ListIterator Inspector::inspect(const metkit::mars::MarsRequest& request) const {

    class NullNotifier : public Notifier {
        void notifyWind() const override {}
    };

    return inspect(request, NullNotifier());
}

ListIterator Inspector::inspect(const metkit::mars::MarsRequest& request, const Notifier& notifyee) const {

    return inspect(request, dbConfig_.schema(), notifyee);
}

void Inspector::visitEntries(const FDBToolRequest& request, EntryVisitor& visitor) const {}

void Inspector::print(std::ostream& out) const {
    out << "Inspector[]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

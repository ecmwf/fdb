// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/database/Inspector.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/MultiRetrieveVisitor.h"
#include "fdb5/rules/Schema.h"

#include "metkit/mars/MarsRequest.h"

#include "eckit/config/Resource.h"
#include "eckit/log/Log.h"

#include <memory>
#include <mutex>

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

ListIterator Inspector::inspect(const metkit::mars::MarsRequest& request) const {
    std::lock_guard lock(mutex_);

    auto iterator = std::make_unique<InspectIterator>();
    MultiRetrieveVisitor visitor(*iterator, databases_, dbConfig_);

    const auto& schema = dbConfig_.schema();
    LOG_DEBUG_LIB(LibFdb5) << "Using schema: " << schema << std::endl;

    schema.expand(request, visitor);

    using QueryIterator = APIIterator<ListElement>;
    return QueryIterator(iterator.release());
}

void Inspector::visitEntries(const FDBToolRequest& request, EntryVisitor& visitor) const {}

void Inspector::print(std::ostream& out) const {
    out << "Inspector[]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/Retriever.h"

#include "eckit/config/Resource.h"
#include "eckit/log/Log.h"
#include "eckit/log/Plural.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Notifier.h"
#include "fdb5/database/MultiRetrieveVisitor.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/rules/Schema.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static void purgeDB(Key& key, DB*& db) {
    Log::debug() << "Purging DB with key " << key << std::endl;
    delete db;
}

Retriever::Retriever(const Config& dbConfig) :
    databases_(Resource<size_t>("fdbMaxOpenDatabases", 16), &purgeDB),
    dbConfig_(dbConfig) {}

Retriever::~Retriever() {
}

eckit::DataHandle* Retriever::retrieve(const metkit::mars::MarsRequest& request) const {

    ListIterator li = inspect(request);

    HandleGatherer result(true);
    ListElement el;
    while (li.next(el)) {
        result.add(el.location()->dataHandle());
    }
    return result.dataHandle();
}

ListIterator Retriever::inspect(const metkit::mars::MarsRequest& request,
                                const Schema& schema,
                                const fdb5::Notifier& notifyee) const {

    auto async_worker = [this, &request, &schema, &notifyee] (Queue<ListElement>& queue) {
        MultiRetrieveVisitor visitor(notifyee, queue, databases_, dbConfig_);

        Log::debug<LibFdb5>() << "Using schema: " << schema << std::endl;

        schema.expand(request, visitor);
    };

    using QueryIterator = APIIterator<ListElement>;
    using AsyncIterator = APIAsyncIterator<ListElement>;

    return QueryIterator(new AsyncIterator(async_worker));
}

ListIterator Retriever::inspect(const metkit::mars::MarsRequest& request) const {

    class NullNotifier : public Notifier {
        void notifyWind() const override {}
    };

    return inspect(request, NullNotifier());
}

ListIterator Retriever::inspect(const metkit::mars::MarsRequest& request, const Notifier& notifyee) const {

    return inspect(request, dbConfig_.schema(), notifyee);
}

void Retriever::visitEntries(const FDBToolRequest &request, EntryVisitor &visitor) const {

}

void Retriever::print(std::ostream &out) const {
    out << "Retriever[]";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

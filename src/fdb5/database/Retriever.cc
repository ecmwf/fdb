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

#include "metkit/MarsRequest.h"

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

eckit::DataHandle *Retriever::retrieve(const metkit::MarsRequest& request,
                                       const Schema& schema,
                                       bool sorted,
                                       const fdb5::Notifier& notifyee) const {

    try {

        HandleGatherer result(sorted);
        MultiRetrieveVisitor visitor(notifyee, result, databases_, dbConfig_);

        Log::debug<LibFdb5>() << "Using schema: " << schema << std::endl;

        schema.expand(request, visitor);

        eckit::Log::userInfo() << "Retrieving " << eckit::Plural(int(result.count()), "field") << std::endl;

        return result.dataHandle();

    } catch (SchemaHasChanged& e) {

        eckit::Log::warning() << e.what() << std::endl;
        eckit::Log::warning() << "Trying with old schema: " << e.path() << std::endl;

        return retrieve(request, Schema(e.path()), sorted, notifyee); // recurse down with the schema from the exception
    }
}

eckit::DataHandle *Retriever::retrieve(const metkit::MarsRequest& request) const {

    class NullNotifier : public Notifier {
        void notifyWind() const override {}
    };

    return retrieve(request, NullNotifier());
}

eckit::DataHandle *Retriever::retrieve(const metkit::MarsRequest& request, const Notifier& notifyee) const {

    bool sorted = false;

    const std::vector<std::string>& sort = request.values("optimise", /* emptyOK */ true);

    if (sort.size() == 1 && sort[0] == "on") {
        sorted = true;
        eckit::Log::userInfo() << "Using optimise" << std::endl;
    }

    Log::debug<LibFdb5>() << "fdb5::Retriever::retrieve() Sorted? " << sorted << std::endl;

    return retrieve(request, dbConfig_.schema(), sorted, notifyee);
}

void Retriever::visitEntries(const FDBToolRequest &request, EntryVisitor &visitor) const {

}

void Retriever::print(std::ostream &out) const {
    out << "Retriever[]";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

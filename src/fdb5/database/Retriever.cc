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
#include "eckit/log/Plural.h"

#include "fdb5/LibFdb.h"
#include "fdb5/database/Notifier.h"
#include "fdb5/database/MultiRetrieveVisitor.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/rules/Schema.h"

#include "marslib/MarsTask.h"

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

eckit::DataHandle *Retriever::retrieve(const MarsTask& task,
                                       const Schema& schema,
                                       bool sorted,
                                       const fdb5::Notifier& notifyee) const {

    try {

        HandleGatherer result(sorted);
        MultiRetrieveVisitor visitor(notifyee, result, databases_, dbConfig_);

        Log::debug<LibFdb>() << "Using schema: " << schema << std::endl;

        schema.expand(task.request(), visitor);

        eckit::Log::userInfo() << "Retrieving " << eckit::Plural(int(result.count()), "field") << std::endl;

        return result.dataHandle();

    } catch (SchemaHasChanged& e) {

        eckit::Log::warning() << e.what() << std::endl;
        eckit::Log::warning() << "Trying with old schema: " << e.path() << std::endl;

        return retrieve(task, Schema(e.path()), sorted, notifyee); // recurse down with the schema from the exception
    }
}

eckit::DataHandle *Retriever::retrieve(const MarsTask &task) const {

    class NotifyClient : public Notifier {
        const MarsTask &task_;
        virtual void notifyWind() const { task_.notifyWinds(); }

      public:
        NotifyClient(const MarsTask &task) : task_(task) {}
    };

    NotifyClient wind(task);
    return retrieve(task, wind);
}

eckit::DataHandle *Retriever::retrieve(const MarsTask &task, const Notifier& notifyee) const {

    bool sorted = false;
    std::vector<std::string> sort;
    task.request().getValues("optimise", sort);

    Log::debug<LibFdb>() << "fdb5::Retriever::retrieve() Sorted? " << sorted << std::endl;

    if (sort.size() == 1 && sort[0] == "on") {
        sorted = true;
        eckit::Log::userInfo() << "Using optimise" << std::endl;
    }

    return retrieve(task, dbConfig_.schema(), sorted, notifyee);
}

void Retriever::visitEntries(const FDBToolRequest &request, EntryVisitor &visitor) const {

}

void Retriever::print(std::ostream &out) const {
    out << "Retriever[]";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

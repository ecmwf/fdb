/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/io/HandleGatherer.h"
#include "fdb5/config/MasterConfig.h"
#include "fdb5/database/Retriever.h"
#include "fdb5/database/RetrieveVisitor.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/database/NotifyWind.h"

#include "marslib/MarsTask.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Retriever::Retriever() {
}

Retriever::~Retriever() {
}

eckit::DataHandle *Retriever::retrieve(const MarsTask &task, const Schema &schema, bool sorted) const {
    HandleGatherer result(sorted);

    class NotifyClient : public NotifyWind {
        const MarsTask& task_;
        virtual void notifyWind() const {
            task_.notifyWinds();
        }
    public:
        NotifyClient(const MarsTask& task): task_(task) {}
    };

    RetrieveVisitor visitor(NotifyClient(task), result);
    schema.expand(task.request(), visitor);

    return result.dataHandle();
}

eckit::DataHandle *Retriever::retrieve(const MarsTask &task) const {
    // Log::info() << std::endl
    //             << "---------------------------------------------------------------------------------------------------"
    //             << std::endl
    //             << std::endl
    //             << *this
    //             << std::endl;

    bool sorted = false;
    std::vector<std::string> sort;
    task.request().getValues("optimise", sort);

    if (sort.size() == 1 && sort[0] == "on") {
        sorted = true;
        eckit::Log::userInfo() << "Using optimise" << std::endl;
    }

    const Schema &schema = MasterConfig::instance().schema();

    // TODO: this logic does not work if a retrieval spans several
    // databases with different schemas. Another SchemaHasChanged will be thrown.
    try {

        return retrieve(task, schema, sorted);

    } catch (SchemaHasChanged &e) {

        eckit::Log::error() << e.what() << std::endl;
        eckit::Log::error() << "Trying with old schema: " << e.path() << std::endl;

        return retrieve(task, Schema(e.path()), sorted);
    }

}

void Retriever::print(std::ostream &out) const {
    out << "Retriever[]";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

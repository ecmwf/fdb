/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/memory/ScopedPtr.h"
#include "eckit/types/Types.h"
#include "eckit/log/Timer.h"

#include "marslib/MarsTask.h"

#include "fdb5/Retriever.h"
#include "fdb5/RetrieveOp.h"
#include "fdb5/MasterConfig.h"
#include "fdb5/DB.h"
#include "fdb5/Key.h"
#include "fdb5/Rule.h"
#include "fdb5/KeywordHandler.h"
#include "fdb5/HandleGatherer.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Retriever::Retriever(const MarsTask& task) :
    task_(task)
{

    /// @note We may want to canonicalise the request immedietly HERE

}

Retriever::~Retriever()
{
}

struct RetrieveCollector : public KeyCollector {

    HandleGatherer& gatherer_;

    std::vector<Op*> opStack_;

    RetrieveCollector(HandleGatherer& gatherer) : gatherer_(gatherer) {
        opStack_.push_back(new RetrieveOp(gatherer));
    }

    virtual void collect(const Key& key0,
                         const Key& key1,
                         const Key& key2) {

    }

    virtual void enter(const std::string& keyword, const std::string& value) {
        opStack_.back()->enter(keyword, value);
    }

    virtual void leave() {
        opStack_.back()->leave();
    }

    virtual void values(const MarsRequest& request, const std::string& keyword, eckit::StringList& values) {

        const KeywordHandler& handler = MasterConfig::lookupHandler(keyword);

        handler.getValues(task_, keyword, values, key);

    }

};


eckit::DataHandle* Retriever::retrieve()
{
    Log::info() << std::endl
                << "---------------------------------------------------------------------------------------------------"
                << std::endl
                << std::endl
                << *this
                << std::endl;

    bool sorted = false;
    std::vector<std::string> sort;
    task_.request().getValues("_sort", sort);

    if(sort.size() == 1 && sort[0] == "1") {
        sorted = true;
    }

    HandleGatherer result(sorted);

    RetrieveCollector c(result);

    const Rules& rules = MasterConfig::instance().rules();
    rules.expand(task_.request(), c);

    return result.dataHandle();
}

void Retriever::print(std::ostream& out) const
{
    out << "Retriever("
        << "task=" << task_
        << "task.request=" << task_.request()
        << ")"
        << std::endl;
}

void Retriever::retrieve(Key& key,
                         const DB& db,
                         Schema::const_iterator pos,
                         Op& op)
{
    if(pos != db.schema().end()) {

        std::vector<std::string> values;

        const std::string& keyword = *pos;

        db.schema().

        handler.getValues(task_, keyword, values, db, key);

        Log::info() << "Expanding keyword (" << keyword << ") with " << values << std::endl;

        if(!values.size()) {
            retrieve(key, db, ++pos, op);
            return;
        }

        eckit::ScopedPtr<Op> newOp( handler.makeOp(task_, op) );

        Schema::const_iterator next = pos;
        ++next;

        for(std::vector<std::string>::const_iterator j = values.begin(); j != values.end(); ++j) {

            op.enter(keyword, *j);

            key.set(keyword, *j);
            retrieve(key, db, next, *newOp);
            key.unset(keyword);

            op.leave();
        }
    }
    else {
        op.execute(task_, key, op);
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

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
#include "fdb5/MasterConfig.h"
#include "fdb5/DB.h"
#include "fdb5/Key.h"
#include "fdb5/Rule.h"
#include "fdb5/KeywordHandler.h"
#include "fdb5/HandleGatherer.h"
#include "fdb5/ReadVisitor.h"

#include "eckit/config/Resource.h"

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

struct RetrieveVisitor : public ReadVisitor {

    Retriever& owner_;

    eckit::ScopedPtr<DB> db_;

    std::string fdbReaderDB_;
    HandleGatherer& gatherer_;


    RetrieveVisitor(Retriever& owner, const MarsTask& task, HandleGatherer& gatherer):
        owner_(owner),
        gatherer_(gatherer) {
        fdbReaderDB_ = eckit::Resource<std::string>("fdbReaderDB","toc.reader");
    }

    ~RetrieveVisitor() {
    }

    // From Visitor

    virtual bool selectDatabase(const Key& key, const Key& full) {
        Log::info() << "selectDatabase " << key << std::endl;
        db_.reset(DBFactory::build(fdbReaderDB_, key));
        if(!db_->open()) {
            Log::info() << "Database does not exists " << key << std::endl;
            return false;
        }
        else {
            return true;
        }
    }

    virtual bool selectIndex(const Key& key, const Key& full) {
        ASSERT(db_);
        Log::info() << "selectIndex " << key << std::endl;
        return db_->selectIndex(key);
    }

    virtual bool selectDatum(const Key& key, const Key& full) {
        ASSERT(db_);
        Log::info() << "selectDatum " << key << ", " << full << std::endl;

        DataHandle* dh = db_->retrieve(key);

        if(dh) {
            gatherer_.add(dh);
        }

        return (dh != 0);
    }

    virtual void values(const MarsRequest& request, const std::string& keyword, eckit::StringList& values) {
        const KeywordHandler& handler = MasterConfig::instance().lookupHandler(keyword);
        handler.getValues(request, keyword, values, owner_.task_, db_.get());
    }

};


eckit::DataHandle* Retriever::retrieve()
{
//    Log::info() << std::endl
//                << "---------------------------------------------------------------------------------------------------"
//                << std::endl
//                << std::endl
//                << *this
//                << std::endl;

    bool sorted = false;
    std::vector<std::string> sort;
    task_.request().getValues("_sort", sort);

    if(sort.size() == 1 && sort[0] == "1") {
        sorted = true;
    }

    HandleGatherer result(sorted);

    RetrieveVisitor visitor(*this, task_, result);

    const Rules& rules = MasterConfig::instance().rules();
    rules.expand(task_.request(), visitor);

    return result.dataHandle();
}

void Retriever::print(std::ostream& out) const
{
    out << "Retriever(" << task_.request()
        << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

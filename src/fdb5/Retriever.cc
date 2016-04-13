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

struct RetrieveVisitor : public ReadVisitor, public RetrieveOpCallback {

    eckit::ScopedPtr<DB> db_;

    std::vector<Op*> op_;
    const MarsTask& task_;

    std::string fdbReaderDB_;
    HandleGatherer& gatherer_;


    RetrieveVisitor(const MarsTask& task, HandleGatherer& gatherer):
        task_(task),
        gatherer_(gatherer) {

        op_.push_back(new RetrieveOp(*this));
        fdbReaderDB_ = eckit::Resource<std::string>("fdbReaderDB","toc.reader");
    }

    ~RetrieveVisitor() {
        for(std::vector<Op*>::const_iterator j = op_.begin(); j != op_.end(); ++j) {
            delete (*j);
        }
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
        Log::info() << "selectIndex " << key << std::endl;
        return db_->selectIndex(key);
    }

    virtual bool selectDatum(const Key& key, const Key& full) {
        Log::info() << "selectDatum " << key << ", " << full << std::endl;

        DataHandle* dh = db_->retrieve(key);

        if(dh) {
            gatherer_.add(dh);
        }

//        ASSERT(op_.size());
//        op_.back()->execute(task_, full, *op_.back());

        return (dh != 0);
    }

    virtual void enterValue(const std::string& keyword, const std::string& value) {
        ASSERT(op_.size() > 1);
        op_[op_.size()-1]->enter(keyword, value);
    }

    virtual void leaveValue() {
        ASSERT(op_.size() > 1);
        op_[op_.size()-1]->leave();
    }

    virtual void enterKeyword(const MarsRequest& request, const std::string& keyword, eckit::StringList& values) {
        const KeywordHandler& handler = MasterConfig::instance().lookupHandler(keyword);
        handler.getValues(request, keyword, values);
        op_.push_back(handler.makeOp(task_, *op_.back()));
    }

    virtual void leaveKeyword() {
        ASSERT(op_.size());
        Op* op = op_.back();
        op_.pop_back();
        delete op;
    }

    // From RetrieveOpCallback

    virtual bool retrieve(const MarsTask& task, const Key& key) {
        DataHandle* dh = db_->retrieve(key);
        if(dh) {
            Log::info() << "Got data for key " << key << std::endl;
            gatherer_.add(dh);
            return true;
        }
        return false;
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

    RetrieveVisitor c(task_, result);

    const Rules& rules = MasterConfig::instance().rules();
    rules.expand(task_.request(), c);

    return result.dataHandle();
}

void Retriever::print(std::ostream& out) const
{
    out << "Retriever(" << task_.request()
        << ")";
}

// void Retriever::retrieve(Key& key,
//                          const DB& db,
//                          Schema::const_iterator pos,
//                          Op& op)
// {
//     if(pos != db.schema().end()) {

//         std::vector<std::string> values;

//         const std::string& keyword = *pos;

//         db.schema().

//         handler.getValues(task_, keyword, values, db, key);

//         Log::info() << "Expanding keyword (" << keyword << ") with " << values << std::endl;

//         if(!values.size()) {
//             retrieve(key, db, ++pos, op);
//             return;
//         }

//         eckit::ScopedPtr<Op> newOp( handler.makeOp(task_, op) );

//         Schema::const_iterator next = pos;
//         ++next;

//         for(std::vector<std::string>::const_iterator j = values.begin(); j != values.end(); ++j) {

//             op.enter(keyword, *j);

//             key.set(keyword, *j);
//             retrieve(key, db, next, *newOp);
//             key.unset(keyword);

//             op.leave();
//         }
//     }
//     else {
//         op.execute(task_, key, op);
//     }
// }

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

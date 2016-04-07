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
#include "eckit/io/MultiHandle.h"
#include "eckit/types/Types.h"
#include "eckit/log/Timer.h"

#include "marslib/MarsTask.h"

#include "fdb5/Retriever.h"
#include "fdb5/RetrieveOp.h"
#include "fdb5/MasterConfig.h"
#include "fdb5/DB.h"
#include "fdb5/Key.h"
#include "fdb5/KeywordHandler.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Retriever::Retriever(const MarsTask& task) :
    task_(task)
{
}

Retriever::~Retriever()
{
}

eckit::DataHandle* Retriever::retrieve()
{
    eckit::ScopedPtr<MultiHandle> result( new MultiHandle() );

    VecDB dbs = MasterConfig::instance().openSessionDBs(task_);

    for( VecDB::const_iterator jdb = dbs.begin(); jdb != dbs.end(); ++jdb) {

        const DB& db = **jdb;

        eckit::ScopedPtr<MultiHandle> partial( new MultiHandle() );

        Key key;

        RetrieveOp op(db, *partial);
        retrieve(key, db.schema(), db.schema().begin(), op);

        if(partial->count()) {
            *result += partial.release();
        }
        else {
            Log::info() << "MultiHandle is empty for DB " << db << std::endl;
        }
    }

    std::vector<std::string> sort;
    task_.request().getValues("_sort", sort);

    /// @TODO MultiHandle isn't calling sort() on children
    ///       This code should not being called yet because client does ask for _sort yet
    if(sort.size() == 1 && sort[0] == "1") {
        result->compress(true);
    }

    return result.release();
}

void Retriever::retrieve(Key& key,
                         const Schema& schema,
                         Schema::const_iterator pos,
                         Op& op)
{
    if(pos != schema.end()) {

        std::vector<std::string> values;

        const std::string& keyword = *pos;
        task_.request().getValues(keyword, values);

        Log::info() << "Expanding keyword (" << keyword << ") with " << values << std::endl;

        ASSERT(values.size());

////    if(!values.size()) { retrieve(key, shema, ++pos, op); return; }

        const KeywordHandler& handler = KeywordHandler::lookup(keyword);

        /// @todo cannocicalisation of values
        //  handler.getValues( task, keyword, values );

        eckit::ScopedPtr<Op> newOp( handler.makeOp(task_, op) );

        Schema::const_iterator next = pos;
        ++next;

        for(std::vector<std::string>::const_iterator j = values.begin(); j != values.end(); ++j) {

            op.enter(keyword, *j);

            key.set(keyword, *j);
            retrieve(key, schema, next, *newOp);
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

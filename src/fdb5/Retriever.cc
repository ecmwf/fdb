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
#include "eckit/log/Timer.h"

#include "marskit/MarsRequest.h"

#include "fdb5/FdbTask.h"
#include "fdb5/Retriever.h"
#include "fdb5/RetrieveOp.h"
#include "fdb5/ForwardOp.h"
#include "fdb5/UVOp.h"
#include "fdb5/MasterConfig.h"
#include "fdb5/DB.h"

using namespace eckit;
using namespace marskit;

namespace fdb {

//----------------------------------------------------------------------------------------------------------------------

Retriever::Retriever(const FdbTask& task) :
    task_(task),
    winds_(task.request())
{
}

Retriever::~Retriever()
{
}

eckit::DataHandle* Retriever::retrieve()
{
    eckit::ScopedPtr<MultiHandle> result( new MultiHandle() );

    VecDB dbs = MasterConfig::instance().visitDBs(task_);

    for( VecDB::const_iterator jdb = dbs.begin(); jdb != dbs.end(); ++jdb) {

        const DB& db = **jdb;

        eckit::ScopedPtr<MultiHandle> partial( new MultiHandle() );

        MarsRequest field;

        RetrieveOp op(db, *partial);
        retrieve(field, db.schema(), 0, op);

        *result += partial.release();
    }

    return result.release();
}

void Retriever::retrieve(marskit::MarsRequest& field,
                         const std::vector<std::string>& schema,
                         size_t pos,
                         Op& op)
{
    if(pos != schema.size()) {

        std::vector<std::string> values;

        const std::string& param = schema[pos];
        task_.request().getValues(param, values);

        /// @TODO once ParamList is typed, we can call:
        ///         values = param->getValues(userReq);
        ///         param->makeOp(op);

        eckit::ScopedPtr<Op> newOp( (param == "param") ?
                                        static_cast<Op*>(new UVOp(op, winds_)) :
                                        static_cast<Op*>(new ForwardOp(op)) );

        for(std::vector<std::string>::const_iterator j = values.begin(); j != values.end(); ++j) {
            field.setValue(param, *j);
            op.descend();
            retrieve(field, schema, pos+1, *newOp);
        }
    }
    else {
        op.execute(task_, field);
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

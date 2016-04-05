/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/io/DataHandle.h"

#include "fdb5/DB.h"
#include "fdb5/RetrieveOp.h"
#include "fdb5/NotFound.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RetrieveOp::RetrieveOp(const DB& db, MultiHandle& result) :
    db_(db),
    result_(result)
{
}

RetrieveOp::~RetrieveOp()
{
}

void RetrieveOp::descend()
{
}

void RetrieveOp::execute(const MarsTask& task, Key& key)
{
    DataHandle* dh = db_.retrieve(task, key);
    if(dh) {
        result_ += dh;
    }
    else {
        fail(task, key);
    }
}

void RetrieveOp::fail(const MarsTask& task, Key& key)
{
    throw NotFound(key);
}

void RetrieveOp::print(std::ostream &out) const
{
    out << "RetrieveOp(db=" << db_ << ",result=" << result_ << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

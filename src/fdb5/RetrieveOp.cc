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

#include "marskit/MarsRequest.h"

#include "fdb5/TOC.h"
#include "fdb5/RetrieveOp.h"
#include "fdb5/NotFound.h"

using namespace eckit;
using namespace marskit;

namespace fdb {

//----------------------------------------------------------------------------------------------------------------------

RetrieveOp::RetrieveOp(const TOC& toc, MultiHandle& result) :
    toc_(toc),
    result_(result)
{
}

RetrieveOp::~RetrieveOp()
{
}

void RetrieveOp::descend()
{
}

void RetrieveOp::execute(const FdbTask& task, marskit::MarsRequest& field)
{
    DataHandle* dh = toc_.retrieve(task, field);
    if(dh) {
        result_ += dh;
    }
    else {
        fail(task, field);
    }
}

void RetrieveOp::fail(const FdbTask& task, marskit::MarsRequest& field)
{
    throw NotFound(field);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

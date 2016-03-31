/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "marskit/MarsRequest.h"

#include "fdb5/ForwardOp.h"

using namespace eckit;
using namespace marskit;

namespace fdb {

//----------------------------------------------------------------------------------------------------------------------

ForwardOp::ForwardOp(Op& parent) :
    parent_(parent)
{
}

ForwardOp::~ForwardOp()
{
}

void ForwardOp::descend()
{
    parent_.descend();
}

void ForwardOp::execute(const FdbTask& task, marskit::MarsRequest& field)
{
    parent_.execute(task, field);
}

void ForwardOp::fail(const FdbTask& task, marskit::MarsRequest& field)
{
    parent_.fail(task, field);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

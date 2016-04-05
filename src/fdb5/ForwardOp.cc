/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"

#include "fdb5/ForwardOp.h"

using namespace eckit;

namespace fdb5 {

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

void ForwardOp::execute(const MarsTask& task, Key& key)
{
    parent_.execute(task, key);
}

void ForwardOp::fail(const MarsTask& task, Key& key)
{
    parent_.fail(task, key);
}

void ForwardOp::print(std::ostream &out) const
{
    out << "ForwardOp(";
    parent_.print(out);
    out << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

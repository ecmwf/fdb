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

void ForwardOp::enter(const std::string& param, const std::string& value) {
    Log::debug() << " > ForwardOp";
    parent_.enter(param, value);
}

void ForwardOp::leave() {
    Log::debug() << " < ForwardOp";
    parent_.leave();
}

void ForwardOp::execute(const MarsTask& task, Key& key, Op& tail)
{
    parent_.execute(task, key, tail);
}

void ForwardOp::fail(const MarsTask& task, Key& key, Op& tail)
{
    parent_.fail(task, key, tail);
}

void ForwardOp::print(std::ostream &out) const
{
    out << "ForwardOp(" << parent_ << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

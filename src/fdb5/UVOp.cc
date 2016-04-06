/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "marslib/MarsTask.h"

#include "fdb5/Key.h"
#include "fdb5/UVOp.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

UVOp::UVOp(const MarsTask& task, Op& parent) :
    parent_(parent),
    userWinds_(task)
{
}

UVOp::~UVOp()
{
}

void UVOp::enter(const std::string& param, const std::string& value) {
    Log::debug() << " > UVOp";
    windsKey_.set(param, value);
    parent_.enter(param, value);
}

void UVOp::leave() {
    Log::debug() << " < UVOp";
}

void UVOp::execute(const MarsTask& task, Key& key, Op& tail)
{
    parent_.execute(task, key, tail);
}

void UVOp::fail(const MarsTask& task, Key& key, Op& tail)
{
    const std::string& param = key.get("param");

    WindsMap::iterator j = winds_.find(windsKey_);

    if(j == winds_.end()) {
        j = winds_.insert( std::make_pair(windsKey_, userWinds_) ).first;
    }

    Winds& winds = j->second;

    bool ok = false;

    if(winds.isU(param)) {

        ASSERT(winds.wantU_);
        ASSERT(!winds.UfromVOD_);

        if(!winds.wantVO_ && !winds.gotVO_) {
            key.set("param", winds.getVO(param));
            execute(task, key, tail);
            winds.gotVO_ = true;
        }

        if(!winds.wantD_ && !winds.gotD_) {
            key.set("param", winds.getD(param));
            execute(task, key, tail);
            winds.gotD_ = true;
        }

        winds.UfromVOD_ = true;
        ok = true;
    }

    if(winds.isV(param)) {

        ASSERT(winds.wantV_);
        ASSERT(!winds.VfromVOD_);

        if(!winds.wantVO_ && !winds.gotVO_) {
            key.set("param", winds.getVO(param));
            execute(task, key, tail);
            winds.gotVO_ = true;
        }

        if(!winds.wantD_ && !winds.gotD_) {
            key.set("param", winds.getD(param));
            execute(task, key, tail);
            winds.gotD_ = true;
        }

        winds.VfromVOD_ = true;
        ok = true;
    }

    if(ok) {
        task.notifyWinds();
    }
    else
    {
        parent_.fail(task, key, tail);
    }

}

void UVOp::print(std::ostream &out) const
{
    out << "UVOp(" << parent_ << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

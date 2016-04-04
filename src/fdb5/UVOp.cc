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

#include "marslib/MarsTask.h"

#include "fdb5/Key.h"
#include "fdb5/UVOp.h"

using namespace eckit;
using namespace marskit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

UVOp::UVOp(Op& parent, const Winds& winds) :
    parent_(parent),
    winds_(winds)
{
}

UVOp::~UVOp()
{
}

void UVOp::descend()
{
    winds_.reset();
    parent_.descend();
}

void UVOp::execute(const MarsTask& task, Key& key)
{
    parent_.execute(task, key);
}

void UVOp::fail(const MarsTask& task, Key& key)
{
    const std::string& param = key.get("param");

    bool ok = false;

    if(winds_.isU(param)) {

        ASSERT(winds_.wantU_);
        ASSERT(!winds_.UfromVOD_);

        if(!winds_.wantVO_ && !winds_.gotVO_) {
            key.set("param", winds_.getVO(param));
            execute(task, key);
            winds_.gotVO_ = true;
        }

        if(!winds_.wantD_ && !winds_.gotD_) {
            key.set("param", winds_.getD(param));
            execute(task, key);
            winds_.gotD_ = true;
        }

        winds_.UfromVOD_ = true;
        ok = true;
    }

    if(winds_.isV(param)) {

        ASSERT(winds_.wantV_);
        ASSERT(!winds_.VfromVOD_);

        if(!winds_.wantVO_ && !winds_.gotVO_) {
            key.set("param", winds_.getVO(param));
            execute(task, key);
            winds_.gotVO_ = true;
        }

        if(!winds_.wantD_ && !winds_.gotD_) {
            key.set("param", winds_.getD(param));
            execute(task, key);
            winds_.gotD_ = true;
        }

        winds_.VfromVOD_ = true;
        ok = true;
    }

    if(ok) {
        task.notifyWinds();
    }
    else
    {
        parent_.fail(task, key);
    }

}

void UVOp::print(std::ostream &out) const
{
    out << "UVOp(";
    parent_.print(out);
    out << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

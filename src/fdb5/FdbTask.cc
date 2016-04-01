/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "marslib/MarsRequest.h"

#include "fdb5/FdbTask.h"

using namespace eckit;
using namespace marskit;

namespace fdb {

//----------------------------------------------------------------------------------------------------------------------

FdbTask::FdbTask(const marskit::MarsRequest& request) :
    request_(request),
    windNotified_(false)
{
}

FdbTask::~FdbTask()
{
}

void FdbTask::notifyWinds() const
{
    if(!windNotified_) {
        Log::notifyClient("wind conversion requested");
        windNotified_ = true;
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

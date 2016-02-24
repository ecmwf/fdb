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
#include "eckit/log/Timer.h"

#include "marslib/MarsRequest.h"

#include "fdb5/Archiver.h"
#include "fdb/FdbApp.h"

using namespace eckit;
//using namespace marskit;

namespace fdb {

//----------------------------------------------------------------------------------------------------------------------

Archiver::Archiver()
{
//    fdb::Location::setup(); // location must be set here for library behavior
}

Archiver::~Archiver()
{
}

void Archiver::archive(const MarsRequest& r, DataHandle& src)
{
    NOTIMP;
}

void Archiver::archive(DataHandle& src)
{
    NOTIMP;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

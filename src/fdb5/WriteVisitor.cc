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

#include "fdb5/WriteVisitor.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

WriteVisitor::WriteVisitor(std::vector<Key>& prev) :
    prev_(prev),
    count_(0)
{
    prev.resize(3);
}

WriteVisitor::~WriteVisitor()
{
}



//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

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
#include "fdb5/Key.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RetrieveOp::RetrieveOp(const DB& db, HandleGatherer& result) :
    db_(db),
    result_(result)
{
}

RetrieveOp::~RetrieveOp()
{
}

void RetrieveOp::enter(const std::string& param, const std::string& value) {
    Log::debug() << " > RetrieveOp" << std::endl;
}

void RetrieveOp::leave() {
    Log::debug() << " < RetrieveOp" << std::endl;
}

void RetrieveOp::execute(const MarsTask& task, Key& key, Op& tail)
{
    DataHandle* dh = db_.retrieve(task, key);
    if(dh) {
        Log::info() << "Got data for key " << key << std::endl;
        result_.add(dh);
    }
    else {
        Log::info() << "Failed to retrieve key " << key << std::endl;
        tail.fail(task, key, tail);
    }
}

void RetrieveOp::fail(const MarsTask& task, Key& key, Op& tail)
{
//    throw NotFound(key, Here());
//
/// @todo Failure is an option :)
}

void RetrieveOp::print(std::ostream &out) const
{
    out << "RetrieveOp(db=" << db_ << ",result=" << result_ << ")";
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

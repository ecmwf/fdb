/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date Nov 2016

#include "fdb5/pmem/PMemIndexLocation.h"

using namespace eckit;


namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------


PMemIndexLocation::PMemIndexLocation(PBranchingNode& node, DataPoolManager& mgr) :
    node_(node),
    poolManager_(mgr) {}


PBranchingNode& PMemIndexLocation::node() const {
    return node_;
}


DataPoolManager& PMemIndexLocation::pool_manager() const {
    return poolManager_;
}

PathName PMemIndexLocation::location() const
{
    return poolManager_.dataPoolPath();
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

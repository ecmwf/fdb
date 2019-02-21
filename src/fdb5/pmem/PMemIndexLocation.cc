/*
 * (C) Copyright 1996- ECMWF.
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
#include "fdb5/pmem/PBranchingNode.h"

#include "pmem/PoolRegistry.h"

using namespace eckit;


namespace fdb5 {
namespace pmem {

// PMemIndexLocation cannot be sensibly reconstructed on a remote.
// Create something that gives info without needing the pmem library, that
// could be remapped into a PMemIndexLocation if we later so chose.

// --> For info purposes we return a TocIndexLocation which has the required
//     components.
// --> Obviously, if this needs to be reconstructed, then we need to do
//     something else magical.

//::eckit::ClassSpec PMemFieldLocation::classSpec_ = {&FieldLocation::classSpec(), "PMemFieldLocation",};
//::eckit::Reanimator<PMemFieldLocation> PMemFieldLocation::reanimator_;

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

PathName PMemIndexLocation::url() const
{
    ::pmem::PersistentPool& pool(::pmem::PoolRegistry::instance().poolFromPointer(&node_));

    return pool.path();
}

IndexLocation* PMemIndexLocation::clone() const {
    return new PMemIndexLocation(node_, poolManager_);
}

void PMemIndexLocation::encode(Stream &) const {
    NOTIMP; // See comment at top of file
}

void PMemIndexLocation::print(std::ostream &out) const
{
    out << "(" << url() << ")";

}

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

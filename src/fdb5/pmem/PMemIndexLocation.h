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

#ifndef fdb5_pmem_PMemIndexLocation_H
#define fdb5_pmem_PMemIndexLocation_H

#include "pmem/PersistentPtr.h"

#include "fdb5/database/IndexLocation.h"


namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------

class PBranchingNode;


class PMemIndexLocation : public IndexLocation {

public: // methods

    PMemIndexLocation(PBranchingNode& node);

    PBranchingNode& node() const;

private: // friends

    PBranchingNode& node_;
};


//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif // fdb5_pmem_PMemIndexLocation_H

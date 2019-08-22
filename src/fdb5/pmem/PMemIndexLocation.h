/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date Nov 2016

#ifndef fdb5_pmem_PMemIndexLocation_H
#define fdb5_pmem_PMemIndexLocation_H

#include "pmem/PersistentPtr.h"

#include "fdb5/database/IndexLocation.h"
#include "fdb5/pmem/DataPoolManager.h"


namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------

class PBranchingNode;


class PMemIndexLocation : public IndexLocation {

public: // methods

    PMemIndexLocation(PBranchingNode& node, DataPoolManager& mgr);

    PBranchingNode& node() const;
    DataPoolManager& pool_manager() const;

    IndexLocation* clone() const override;

    eckit::PathName url() const override;

protected: // For Streamable (see comments. This is a bit odd).

    virtual void encode(eckit::Stream&) const;

private: // methods

    virtual void print(std::ostream &out) const;

private: // friends

    PBranchingNode& node_;
    DataPoolManager& poolManager_;
};


//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif // fdb5_pmem_PMemIndexLocation_H

/*
 * (C) Copyright 1996-2015 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Sep 2016


#ifndef fdb5_pmem_PRoot_H
#define fdb5_pmem_PRoot_H

#include "eckit/memory/NonCopyable.h"
#include "eckit/types/FixedString.h"
#include "eckit/types/Types.h"

#include "pmem/AtomicConstructor.h"
#include "pmem/PersistentPtr.h"
#include "pmem/PersistentPODVector.h"

#include "fdb5/pmem/PBranchingNode.h"
#include "fdb5/pmem/PDataNode.h"

#include <ctime>


namespace fdb5 {
namespace pmem {

// -------------------------------------------------------------------------------------------------

// N.B. This is to be stored in PersistentPtr --> NO virtual behaviour.

class PRoot {

public: // Construction objects

    class Constructor : public ::pmem::AtomicConstructor<PRoot> {
    public: // methods
        Constructor();
        virtual void make(PRoot& object) const;
    };

public: // methods

    bool valid() const;

    const time_t& created() const;

    ::pmem::PersistentPtr<PBranchingNode> getBranchingNode(const Key& key) const;
    PBranchingNode& getCreateBranchingNode(const Key& key);

    void print(std::ostream& s) const;

private: // members

    eckit::FixedString<8> tag_;

    unsigned short int version_;

    /// Store the size of the PMemRoot object. Allows some form of sanity check
    /// when it is being opened, that everything is safe.
    unsigned short int rootSize_;

    time_t created_;

    long createdBy_;

    ::pmem::PersistentPtr<PBranchingNode> rootNode_;

    /// Keep track of how many data pools are in use. Ensure locking whenever updating this variable.

    ::pmem::PersistentMutex mutex_;
    ::pmem::PersistentPODVector<uint64_t> dataPoolUUIDs_;

    ::pmem::PersistentPtr< ::pmem::PersistentBuffer> schema_;

private: // friends

    friend class DataPoolManager;

    friend std::ostream& operator<<(std::ostream& s, const PRoot& r) { r.print(s); return s; }
};


// A consistent definition of the tag for comparison purposes.
const eckit::FixedString<8> PRootTag = "99FDB599";
const unsigned short int PRootVersion = 1;


// -------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif // fdb5_pmem_PRoot_H

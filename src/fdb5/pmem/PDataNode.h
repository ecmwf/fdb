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


#ifndef fdb5_pmem_PDataNode_H
#define fdb5_pmem_PDataNode_H

#include "eckit/types/Types.h"

#include "pmem/AtomicConstructor.h"
#include "pmem/AtomicConstructorCast.h"
#include "pmem/PersistentVector.h"

#include "fdb5/pmem/PBaseNode.h"


namespace fdb5 {

class Key;

namespace pmem {

// -------------------------------------------------------------------------------------------------

// N.B. This is to be stored in PersistentPtr --> NO virtual behaviour.

class PDataNode : public PBaseNode {

public: // Construction objects

    class Constructor : public ::pmem::AtomicConstructor<PDataNode>, public PBaseNode::Constructor {
    public: // methods
        Constructor(const KeyType& key, const ValueType& value, const void* data, eckit::Length length);
        virtual size_t size() const;
        virtual void make(PDataNode& object) const;
    private:
        eckit::Length length_;
        const void* data_;
    };

    typedef ::pmem::AtomicConstructorCast<PDataNode, PBaseNode> BaseConstructor;

private: // members

    eckit::Length length_;

    // We use a length of 8 to ensure (a) this is legal c++, and (b) there is no possibility of the complier
    // adding unecessary padding due to an unreasonable desire for structure alignment.
    char data_[8];

};

// -------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif // fdb5_pmem_PDataNode_H

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
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

class CanonicalKey;
class EntryVisitor;

namespace pmem {

class DataPoolManager;

// -------------------------------------------------------------------------------------------------

// N.B. This is to be stored in PersistentPtr --> NO virtual behaviour.

class PDataNode : public PBaseNode {

public: // types

    typedef ::pmem::AtomicConstructor4<PDataNode, KeyType,
                                                  ValueType,
                                                  const void*,
                                                  eckit::Length> Constructor;

    typedef ::pmem::AtomicConstructorCast<PDataNode, PBaseNode> BaseConstructor;

public: // methods

    PDataNode(const KeyType& key, const ValueType& value, const void* data, eckit::Length length);

    const void* data() const;

    eckit::Length length() const;

    static size_t data_size(eckit::Length length);

private: // members

    eckit::Length length_;

    // We use a length of 8 to ensure (a) this is legal c++, and (b) there is no possibility of the complier
    // adding unecessary padding due to an unreasonable desire for structure alignment.
    char data_[8];

};

// -------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

// -------------------------------------------------------------------------------------------------

namespace pmem {

template<>
inline size_t AtomicConstructor4Base<fdb5::pmem::PDataNode,
                                                fdb5::pmem::PBaseNode::KeyType,
                                                fdb5::pmem::PBaseNode::ValueType,
                                                const void*,
                                                eckit::Length>::size() const {
    return fdb5::pmem::PDataNode::data_size(x4_);

}

}


#endif // fdb5_pmem_PDataNode_H

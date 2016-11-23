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


#ifndef fdb5_pmem_PBranchingNode_H
#define fdb5_pmem_PBranchingNode_H

#include "eckit/types/Types.h"

#include "pmem/AtomicConstructor.h"
#include "pmem/AtomicConstructorCast.h"
#include "pmem/PersistentVector.h"
#include "pmem/PersistentMutex.h"
#include "pmem/PersistentBuffer.h"

#include "fdb5/pmem/PBaseNode.h"
#include "fdb5/database/Key.h"


namespace fdb5 {
namespace pmem {

class DataPoolManager;

// -------------------------------------------------------------------------------------------------

// N.B. This is to be stored in PersistentPtr --> NO virtual behaviour.

class PBranchingNode : public PBaseNode {

public: // Construction objects

    /// The standard constructor just creates a node

    class Constructor : public ::pmem::AtomicConstructor<PBranchingNode>, public PBaseNode::Constructor {
    public: // methods
        Constructor(const KeyType& key, const ValueType& value);
        virtual void make(PBranchingNode& object) const;
    };

    /// The Index constructor creates a BranchingNode, and then recursively creates required
    /// subnodes until the key is exhausted with a BranchingNode.

    class IndexConstructor : public PBranchingNode::Constructor {
    public: // methods
        IndexConstructor(Key::const_iterator key,
                         Key::const_iterator end,
                         PBranchingNode** const indexNode);
        virtual void make(PBranchingNode& object) const;

    private: // members
        Key::const_iterator keysIterator_;
        Key::const_iterator endIterator_;
        PBranchingNode** const indexNode_;
    };

    typedef ::pmem::AtomicConstructorCast<PBranchingNode, PBaseNode> BaseConstructor;

public: // methods

    /// Obtain a branching (intermediate) node in the tree. If it doesn't exist, then
    /// create it.
    /// @arg key - The key of the node to create or get _relative_to_the_current_node_
    /// @return A reference to the newly created, or found, node
    /// @note This is primarily in place to support the getIndex functionality.
    PBranchingNode& getCreateBranchingNode(const Key& key);

    void insertDataNode(const Key& key, const ::pmem::PersistentPtr<PDataNode>& dataNode);

    /// Obtain a branching (intermediate) node in the tree. If it doesn't exist, then
    /// a null pointer is returned.
    /// @arg key - The key of the node to create or get _relative_to_the_current_node_
    /// @return A PersistentPtr to the node, or a null() pointer.
    ::pmem::PersistentPtr<PBranchingNode> getBranchingNode(const Key& key);

private: // methods

    PBranchingNode& getCreateBranchingNode(Key::const_iterator start,
                                           Key::const_iterator end);

private: // members

    ::pmem::PersistentVector<PBaseNode> nodes_;

    ::pmem::PersistentMutex mutex_;

    // TODO: Do we want to have a separate IndexNode?
    ::pmem::PersistentPtr< ::pmem::PersistentBuffer> axis_;

private: // friends

    friend class PMemIndex;
};


// -------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5


#endif // fdb5_pmem_PBranchingNode_H

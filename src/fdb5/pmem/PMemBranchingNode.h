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


#ifndef fdb5_pmem_PMemBranchingNode_H
#define fdb5_pmem_PMemBranchingNode_H

#include "eckit/types/Types.h"

#include "pmem/AtomicConstructor.h"
#include "pmem/PersistentVector.h"
#include "pmem/PersistentMutex.h"

#include "fdb5/pmem/PMemBaseNode.h"


namespace fdb5 {

class Key;

// -------------------------------------------------------------------------------------------------

// N.B. This is to be stored in PersistentPtr --> NO virtual behaviour.

class PMemBranchingNode : public PMemBaseNode {

public: // Construction objects

    /// The standard constructor just creates a node

    class Constructor : public pmem::AtomicConstructor<PMemBranchingNode>, public PMemBaseNode::Constructor {
    public: // methods
        Constructor(const KeyType& key, const ValueType& value);
        virtual void make(PMemBranchingNode& object) const;
    };

    /// The Index constructor creates a BranchingNode, and then recursively creates required
    /// subnodes until the key is exhausted with a BranchingNode.

    class IndexConstructor : public PMemBranchingNode::Constructor {
    public: // methods
        IndexConstructor(eckit::StringDict::const_iterator key,
                         eckit::StringDict::const_iterator end,
                         PMemBranchingNode** const indexNode);
        virtual void make(PMemBranchingNode& object) const;

    private: // members
        eckit::StringDict::const_iterator keysIterator_;
        eckit::StringDict::const_iterator endIterator_;
        PMemBranchingNode** const indexNode_;
    };

    /// A wrapper to allow us to pass a PMemBranchingNode constructor into allocate for the base class
    /// TODO: Can we create a more generic templated AtomicConstructor-caster.
    class BaseConstructor : public pmem::AtomicConstructor<PMemBaseNode> {
    public: // methods
        BaseConstructor(const PMemBranchingNode::Constructor& ctr);
        virtual void make(PMemBaseNode& object) const;
        virtual size_t size() const;

    private: // members
        const PMemBranchingNode::Constructor& ctr_;
    };

public: // methods

    /// Obtain a branching (intermediate) node in the tree. If it doesn't exist, then
    /// create it.
    /// @arg key - The key of the node to create or get _relative_to_the_current_node_
    /// @return A reference to the newly created, or found, node
    /// @note This is primarily in place to support the getIndex functionality.
    PMemBranchingNode& getCreateBranchingNode(const Key& key);

    PMemDataNode& createDataNode(const Key& key, const void* data, eckit::Length length);

private: // methods

    PMemBranchingNode& getCreateBranchingNode(eckit::StringDict::const_iterator start,
                                              eckit::StringDict::const_iterator end);

private: // members

    pmem::PersistentVector<PMemBaseNode> nodes_;

    pmem::PersistentMutex mutex_;

};

// -------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_pmem_PMemBranchingNode_H

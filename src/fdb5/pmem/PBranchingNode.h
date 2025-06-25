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


#ifndef fdb5_pmem_PBranchingNode_H
#define fdb5_pmem_PBranchingNode_H

#include "pmem/AtomicConstructor.h"
#include "pmem/AtomicConstructorCast.h"
#include "pmem/PersistentBuffer.h"
#include "pmem/PersistentMutex.h"
#include "pmem/PersistentVector.h"

#include "eckit/types/Types.h"

#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/pmem/PBaseNode.h"


namespace fdb5 {

class EntryVisitor;
class Schema;
class IndexBase;

namespace pmem {

class DataPoolManager;

// -------------------------------------------------------------------------------------------------

// N.B. This is to be stored in PersistentPtr --> NO virtual behaviour.

class PBranchingNode : public PBaseNode {

public:  // types

    typedef std::vector<std::pair<std::string, std::string> > KeyValueVector;

public:  // Construction objects

    /// The Index constructor creates a BranchingNode, and then recursively creates required
    /// subnodes until the key is exhausted with a BranchingNode.

    typedef ::pmem::AtomicConstructor2<PBranchingNode, KeyType, ValueType> NodeConstructor;

    typedef ::pmem::AtomicConstructorCast<PBranchingNode, PBaseNode> BaseConstructor;

public:  // methods

    PBranchingNode(const KeyType& key, const ValueType& value);

    /// Recursively create a chain of nodes, returning a pointer to the tail of the chain
    PBranchingNode(KeyValueVector::const_iterator start, KeyValueVector::const_iterator end,
                   PBranchingNode** const tailNode);

    /// Obtain a branching (intermediate) node in the tree. If it doesn't exist, then
    /// create it.
    /// @arg key - The key of the node to create or get _relative_to_the_current_node_
    /// @return A reference to the newly created, or found, node
    /// @note This is primarily in place to support the getIndex functionality.
    PBranchingNode& getCreateBranchingNode(const Key& key);

    void insertDataNode(const Key& key, const ::pmem::PersistentPtr<PDataNode>& dataNode, DataPoolManager& mgr);

    ::pmem::PersistentPtr<PDataNode> getDataNode(const Key& key, DataPoolManager& mgr) const;

    /// Obtain a branching (intermediate) node in the tree. If it doesn't exist, then
    /// a null pointer is returned.
    /// @arg key - The key of the node to create or get _relative_to_the_current_node_
    /// @return A PersistentPtr to the node, or a null() pointer.
    ::pmem::PersistentPtr<PBranchingNode> getBranchingNode(const Key& key) const;

    void visitLeaves(EntryVisitor& visitor, DataPoolManager& mgr, std::vector<Key>& keys, size_t depth,
                     Index index = Index());

    /// Returns true if this node is (or at least has been used as) an Index.
    bool isIndex() const;

private:  // methods

    PBranchingNode& getCreateBranchingNode(const KeyValueVector& identifier);

protected:  // members

    ::pmem::PersistentVector<PBaseNode> nodes_;

    mutable ::pmem::PersistentMutex mutex_;

    // TODO: Do we want to have a separate IndexNode?
    ::pmem::PersistentPtr< ::pmem::PersistentBuffer> axis_;

private:  // friends

    friend class PMemIndex;
};


// -------------------------------------------------------------------------------------------------

}  // namespace pmem
}  // namespace fdb5


#endif  // fdb5_pmem_PBranchingNode_H

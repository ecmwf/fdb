/*
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-3.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date   Feb 2016

#include "eckit/log/Log.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/types/Types.h"

#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/pmem/DataPoolManager.h"
#include "fdb5/pmem/PBranchingNode.h"
#include "fdb5/pmem/PDataNode.h"
#include "fdb5/pmem/PMemIndex.h"
#include "fdb5/pmem/PMemFieldLocation.h"
#include "fdb5/pmem/PRoot.h"
#include "fdb5/rules/Schema.h"

#include "pmem/PoolRegistry.h"

#include <unistd.h>

using namespace eckit;
using namespace pmem;


namespace fdb5 {
namespace pmem {

// -------------------------------------------------------------------------------------------------


PBranchingNode::PBranchingNode(const KeyType &key, const ValueType &value) :
    PBaseNode(BRANCHING_NODE, key, value) {}


// -------------------------------------------------------------------------------------------------


PBranchingNode& PBranchingNode::getCreateBranchingNode(const CanonicalKey& key) {

    // Extract an _ordered_ sequence of key-values pairs to use to identify the node.
    // Order comes (originally) from the schema.
    // The vector names() is constructed as the Rules ar matched (see Rule.cc:216)

    const StringList& ordered_keys(key.names());
    KeyValueVector identifier;

    identifier.reserve(ordered_keys.size());
    for (StringList::const_iterator it = ordered_keys.begin(); it != ordered_keys.end(); ++it)
        identifier.push_back(std::make_pair(*it, key.value(*it)));

    return getCreateBranchingNode(identifier);
}


PBranchingNode& PBranchingNode::getCreateBranchingNode(const KeyValueVector& identifier) {

    PBranchingNode* current = this;

    for (KeyValueVector::const_iterator it = identifier.begin(); it != identifier.end(); ++it) {

        // We don't want two processes to simultaneously create two same-named branches. So one processs cannot be
        // checking if a branch exists whilst the other might be creating it.
        AutoLock<PersistentMutex> lock(current->mutex_);

        // Search _backwards_ through the subnodes to find the element we are looking for (this permits newly written
        // fields to mask existing ones without actually overwriting the data and making it irretrievable).

        int i = current->nodes_.size() - 1;
        for (; i >= 0; --i) {

            PersistentPtr<PBaseNode> subnode = current->nodes_[i];

            if (subnode->matches(it->first, it->second)) {

                // Given that we are operating inside a schema, if this matches then it WILL be of the
                // correct type --> we can request it directly.
                ASSERT(subnode->isBranchingNode());
                current = &subnode->asBranchingNode();
                break;
            }
        }

        // Create the node (and all contained sub-nodes) if it hasn't been found. Note that we
        // start at the first

        if (i < 0) {

            PersistentPool& pool(::pmem::PoolRegistry::instance().poolFromPointer(this));

            for (; it != identifier.end(); it++) {
                PersistentPtr<PBaseNode> newNode;
                newNode.allocate_ctr(pool, BaseConstructor(PBranchingNode::NodeConstructor(it->first, it->second)));
                current->nodes_.push_back_elem(newNode);
                current = &newNode->asBranchingNode();
            }

            break;
        }
    }

    return *current;
}


::pmem::PersistentPtr<PDataNode> PBranchingNode::getDataNode(const CanonicalKey& key, DataPoolManager& mgr) const {

    // Extract an _ordered_ sequence of key-values pairs to use to identify the node.
    // Order comes (originally) from the schema.
    // The vector names() is constructed as the Rules ar matched (see Rule.cc:216)

    const StringList& ordered_keys(key.names());
    KeyValueVector identifier;

    identifier.reserve(ordered_keys.size());
    for (StringList::const_iterator it = ordered_keys.begin(); it != ordered_keys.end(); ++it)
        identifier.push_back(std::make_pair(*it, key.value(*it)));

    // And find and return the node!

    PersistentPtr<PDataNode> ret;
    const PBranchingNode* current = this;

    for (KeyValueVector::const_iterator it = identifier.begin(); it != identifier.end(); ++it) {

        AutoLock<PersistentMutex> lock(current->mutex_);

        // Search _backwards_ through the subnodes to find the element we are looking for (this permits newly written
        // fields to mask existing ones without actually overwriting the data and making it irretrievable).

        int i = current->nodes_.size() - 1;
        for (; i >= 0; --i) {

            PersistentPtr<PBaseNode> subnode = current->nodes_[i];

            // This breaks a bit of the encapsulation, but hook in here to check that the relevant
            // pools are loaded. We can't do this earlier, as the data is allowed to be changing
            // up to this point...

            mgr.ensurePoolLoaded(subnode.uuid());

            if (subnode->matches(it->first, it->second)) {

                // The last element in the chain is a data node, otherwise a branching node

                KeyValueVector::const_iterator next = it;
                ++next;
                if (next == identifier.end()) {
                    ASSERT(subnode->isDataNode());
                    ret = subnode.as<PDataNode>();
                } else {
                    ASSERT(subnode->isBranchingNode());
                    current = &subnode->asBranchingNode();
                }

                break;
            }
        }

        // We have failed to find the relevant node. Oops.
        if (i < 0) {
            ret.nullify();
            break;
        }
    }

    return ret;
}


::pmem::PersistentPtr<PBranchingNode> PBranchingNode::getBranchingNode(const CanonicalKey& key) const {

    // Extract an _ordered_ sequence of key-values pairs to use to identify the node.
    // Order comes (originally) from the schema.
    // The vector names() is constructed as the Rules ar matched (see Rule.cc:216)

    const StringList& ordered_keys(key.names());
    KeyValueVector identifier;

    identifier.reserve(ordered_keys.size());
    for (StringList::const_iterator it = ordered_keys.begin(); it != ordered_keys.end(); ++it)
        identifier.push_back(std::make_pair(*it, key.value(*it)));

    // need to ensure that the underlying vector doesn't change too much while we are
    // reading it (there might be a purge!)

    PersistentPtr<PBranchingNode> ret;
    const PBranchingNode* current = this;

    for (KeyValueVector::const_iterator it = identifier.begin(); it != identifier.end(); ++it) {

        //TODO: Is this locking overzealous?

        AutoLock<PersistentMutex> lock(current->mutex_);

        // Search _backwards_ through the subnodes to find the element we are looking for (this permits newly written
        // fields to mask existing ones without actually overwriting the data and making it irretrievable).

        int i = current->nodes_.size() - 1;
        for (; i >= 0; --i) {

            PersistentPtr<PBaseNode> subnode = current->nodes_[i];

            if (subnode->matches(it->first, it->second)) {

                // Given that we are operating inside a schema, if this matches then it WILL be of the
                // correct type --> we can request it directly.
                ASSERT(subnode->isBranchingNode());

                ret = subnode.as<PBranchingNode>();
                current = &subnode->asBranchingNode();
                break;
            }
        }

        // We have failed to find the relevant node. Oops.
        if (i < 0) {
            ret.nullify();
            break;
        }
    }

    return ret;
}


void PBranchingNode::insertDataNode(const CanonicalKey& key, const PersistentPtr<PDataNode>& dataNode, DataPoolManager& mgr) {

    // Obtain the _parent_ branching node - the final element in the chain identifies the
    // specific data node, and so should be excluded from this first operation.

    const StringList& ordered_keys(key.names());
    KeyValueVector parentIdentifier;

    StringList::const_iterator end = ordered_keys.end();
    --end;

    parentIdentifier.reserve(ordered_keys.size());
    for (StringList::const_iterator it = ordered_keys.begin(); it != end; ++it)
        parentIdentifier.push_back(std::make_pair(*it, key.value(*it)));

    PBranchingNode& dataParent(getCreateBranchingNode(parentIdentifier));

    // Some sanity checking

    std::string k = ordered_keys[ordered_keys.size()-1];
    std::string v = key.value(k);
    ASSERT(dataNode->matches(k, v));

    // Lock the parent node for editing.

    AutoLock<PersistentMutex> lock(dataParent.mutex_);


    // Check that we will only mask PDataNodes if there is a matching one. We don't want to block
    // off whole sections of the tree because someone has gone off-schema.

    for (size_t i = 0; i < dataParent.nodes_.size(); i++) {

        // This breaks a bit of the encapsulation, but hook in here to check that the relevant
        // pools are loaded. We can't do this earlier, as the data is allowed to be changing
        // up to this point...

        mgr.ensurePoolLoaded(dataParent.nodes_[i].uuid());

        if (dataParent.nodes_[i]->matches(k, v))
            ASSERT(dataParent.nodes_[i]->isDataNode());
    }

    // And then append the data node to that one.

    dataParent.nodes_.push_back_elem(dataNode.as<PBaseNode>());
}

#if 0

/// visitLeaves should NOT skip out masked data. It is used for enumerating fdb-list, and listing dupliactes, etc.

void PBranchingNode::visitLeaves(EntryVisitor &visitor,
                                 DataPoolManager& mgr,
                                 std::vector<CanonicalKey>& keys,
                                 size_t depth,
                                 Index index) {

    // Get a list of sub-nodes in memory before starting to visit them, so that we can
    // release the lock for further writers.
    // Use this opportunity to de-deplicate the entries.

    if (!(key().empty() && value().empty())) {
        keys.back().push(key(), value());
        if (isIndex()) {

            index = Index(new PMemIndex(keys.back(), *this, mgr));
            keys.push_back(CanonicalKey());
            depth++;
        }
    }

    std::map<std::string, PBranchingNode*> subtrees;
    std::map<std::string, PersistentPtr<PDataNode> > leaves;

    {
        AutoLock<PersistentMutex> lock(mutex_);

        int i = nodes_.size() - 1;
        for (; i >= 0; --i) {

            PersistentPtr<PBaseNode> subnode = nodes_[i];

            // This breaks a bit of the encapsulation, but hook in here to check that the relevant
            // pools are loaded. We can't do this earlier, as the data is allowed to be changing
            // up to this point...

            mgr.ensurePoolLoaded(subnode.uuid());

            std::string kv = subnode->key() + ":" + subnode->value();

            if (subnode->isDataNode()) {

                if (leaves.find(kv) == leaves.end())
                    leaves[kv] = subnode.as<PDataNode>();

            } else {

                // n.b. Should be no masked subtrees, and no mixed data/trees
                ASSERT(subnode->isBranchingNode());
                ASSERT(subtrees.find(kv) == subtrees.end());
                ASSERT(leaves.find(kv) == leaves.end());
                subtrees[kv] = &(subnode->asBranchingNode());
            }
        }
    }

    // Visit the leaves
    std::map<std::string, PersistentPtr<PDataNode> >::const_iterator it_dt = leaves.begin();
    for (; it_dt != leaves.end(); ++it_dt) {

        ASSERT(!index.null());

        // we do the visitation from here, not from PDataNode::visit, as the PMemFieldLocation needs
        // the PersistentPtr, not the "this" pointer.
        keys.back().push(it_dt->second->key(), it_dt->second->value());
        Field field(PMemFieldLocation(it_dt->second, mgr.getPool(it_dt->second.uuid())));

        visitor.visit(index, field,"Index fingerprint unused", keys.back().valuesToString());

        keys.back().pop(it_dt->second->key());
    }

    // Recurse down the trees
    std::map<std::string, PBranchingNode*>::const_iterator it_br = subtrees.begin();
    for (; it_br != subtrees.end(); ++it_br) {
        it_br->second->visitLeaves(visitor, mgr, keys, depth, index);
    }

    if (!(key().empty() && value().empty())) {
        if (isIndex())
            keys.pop_back();
        keys.back().pop(key());
    }
}
#endif


void PBranchingNode::visitLeaves(EntryVisitor &visitor,
                                 DataPoolManager& mgr,
                                 std::vector<CanonicalKey>& keys,
                                 size_t depth,
                                 Index index) {

    // Get a list of sub-nodes in memory before starting to visit them, so that we can
    // release the lock for further writers.
    // Use this opportunity to de-deplicate the entries.

    if (!(key().empty() && value().empty())) {
        keys.back().push(key(), value());
        if (isIndex()) {

            index = Index(new PMemIndex(keys.back(), *this, mgr));

            visitor.visitIndex(index);

            // If we are not visiting right to the leaves, there is no need to go
            // any further
            if (!visitor.visitEntries()) {
                keys.back().pop(key());
                return;
            }

            keys.push_back(CanonicalKey());
            depth++;
        }
    }

    std::vector<PBranchingNode*> subtrees;
    std::vector<PersistentPtr<PDataNode> > leaves;

    {
        AutoLock<PersistentMutex> lock(mutex_);

        int i = nodes_.size() - 1;
        for (; i >= 0; --i) {

            PersistentPtr<PBaseNode> subnode = nodes_[i];

            // This breaks a bit of the encapsulation, but hook in here to check that the relevant
            // pools are loaded. We can't do this earlier, as the data is allowed to be changing
            // up to this point...

            mgr.ensurePoolLoaded(subnode.uuid());

            std::string kv = subnode->key() + ":" + subnode->value();

            if (subnode->isDataNode()) {

                // This would do masking...
                //if (leaves.find(kv) == leaves.end())
                //    leaves[kv] = subnode.as<PDataNode>();
                leaves.push_back(subnode.as<PDataNode>());

            } else {

                // n.b. Should be no masked subtrees, and no mixed data/trees
                ASSERT(subnode->isBranchingNode());
//                ASSERT(subtrees.find(kv) == subtrees.end());
//                ASSERT(leaves.find(kv) == leaves.end());
                subtrees.push_back(&subnode->asBranchingNode());
//                subtrees[kv] = &(subnode->asBranchingNode());
            }
        }
    }

    // Visit the leaves
    std::vector<PersistentPtr<PDataNode> >::const_iterator it_dt = leaves.begin();
    for (; it_dt != leaves.end(); ++it_dt) {

        ASSERT(!index.null());

        // we do the visitation from here, not from PDataNode::visit, as the PMemFieldLocation needs
        // the PersistentPtr, not the "this" pointer.
        keys.back().push((*it_dt)->key(), (*it_dt)->value());
        Field field(PMemFieldLocation(*it_dt, mgr.getPool(it_dt->uuid())));

        visitor.visitDatum(field, keys.back().valuesToString());

        keys.back().pop((*it_dt)->key());
    }

    // Recurse down the trees
    std::vector<PBranchingNode*>::const_iterator it_br = subtrees.begin();
    for (; it_br != subtrees.end(); ++it_br) {
        (*it_br)->visitLeaves(visitor, mgr, keys, depth, index);
    }

    if (!(key().empty() && value().empty())) {
        if (isIndex()) {
            keys.pop_back();
        }
        keys.back().pop(key());
    }
}


bool PBranchingNode::isIndex() const {

    // An index will have an axis object. Couple these concepts
    return !axis_.null();
}


// -------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace tree

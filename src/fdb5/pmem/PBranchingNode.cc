/*
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-3.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Feb 2016

#include "eckit/io/DataBlob.h"
#include "eckit/log/Log.h"
#include "eckit/parser/JSONDataBlob.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/types/Types.h"

#include "fdb5/database/Key.h"
#include "fdb5/pmem/DataPoolManager.h"
#include "fdb5/pmem/PBranchingNode.h"
#include "fdb5/pmem/PDataNode.h"
#include "fdb5/pmem/PRoot.h"

#include <unistd.h>

using namespace eckit;
using namespace pmem;


namespace fdb5 {
namespace pmem {

// -------------------------------------------------------------------------------------------------

PBranchingNode::Constructor::Constructor(const KeyType& key, const ValueType& value) :
    PBaseNode::Constructor(BRANCHING_NODE, key, value) {
    Log::error() << "Constructor(" << key << ": " << value << ")" << std::endl;
}

void PBranchingNode::Constructor::make(PBranchingNode& object) const {
    constructBase(object);
    object.nodes_.nullify();
    object.axis_.nullify();
}


PBranchingNode::IndexConstructor::IndexConstructor(KeyValueVector::const_iterator it,
                                                   KeyValueVector::const_iterator end,
                                                   PBranchingNode** const indexNode) :
    PBranchingNode::Constructor(it->first, it->second),
    keysIterator_(it),
    endIterator_(end),
    indexNode_(indexNode) {
    Log::error() << "IndexConstructor(it)" << std::endl;
}


void PBranchingNode::IndexConstructor::make(PBranchingNode& object) const {
    PBranchingNode::Constructor::make(object);

    // Store the node being created as the indexNode. If further contained nodes are created, the
    // pointer will be (correctly) replaced with the deepest node.
    *indexNode_ = &object;

    // Instantiate nodes recursively until they are all filled

    KeyValueVector::const_iterator next = keysIterator_;
    next++;
    if (next != endIterator_) {
        object.nodes_.push_back_ctr(BaseConstructor(IndexConstructor(next, endIterator_, indexNode_)));
    }
}

// -------------------------------------------------------------------------------------------------


PBranchingNode& PBranchingNode::getCreateBranchingNode(const Key& key) {

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
            current->nodes_.push_back_ctr(BaseConstructor(PBranchingNode::IndexConstructor(it, identifier.end(), &current)));
            break;
        }
    }

    return *current;
}


::pmem::PersistentPtr<PDataNode> PBranchingNode::getDataNode(const Key& key, DataPoolManager& mgr) const {

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


::pmem::PersistentPtr<PBranchingNode> PBranchingNode::getBranchingNode(const Key& key) const {

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


void PBranchingNode::insertDataNode(const Key& key, const PersistentPtr<PDataNode>& dataNode) {

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

    // And then append the data node to that one.

    std::string k = ordered_keys[ordered_keys.size()-1];
    ASSERT(dataNode->matches(k, key.value(k)));

    AutoLock<PersistentMutex> lock(dataParent.mutex_);

    dataParent.nodes_.push_back_elem(dataNode.as<PBaseNode>());
}


// -------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace tree

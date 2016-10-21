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
    Log::info() << "Base constructed" << std::endl;
    object.nodes_.nullify();
}


PBranchingNode::IndexConstructor::IndexConstructor(Key::const_iterator it,
                                                   Key::const_iterator end,
                                                   PBranchingNode** const indexNode) :
    PBranchingNode::Constructor(it->first, it->second),
    keysIterator_(it),
    endIterator_(end),
    indexNode_(indexNode){
    Log::error() << "IndexConstructor(it)" << std::endl;
}


void PBranchingNode::IndexConstructor::make(PBranchingNode& object) const {
    PBranchingNode::Constructor::make(object);

    // Store the node being created as the indexNode. If further contained nodes are created, the
    // pointer will be (correctly) replaced with the deepest node.
    *indexNode_ = &object;

    // Instantiate nodes recursively until they are all filled

    Key::const_iterator next = keysIterator_;
    next++;
    if (next != endIterator_) {
        object.nodes_.push_back(BaseConstructor(IndexConstructor(next, endIterator_, indexNode_)));
    }
}

// -------------------------------------------------------------------------------------------------


PBranchingNode& PBranchingNode::getCreateBranchingNode(const Key& key) {

    return getCreateBranchingNode(key.begin(), key.end());
}


PBranchingNode& PBranchingNode::getCreateBranchingNode(Key::const_iterator start,
                                                       Key::const_iterator end) {

    PBranchingNode* current = this;

    for (Key::const_iterator it = start; it != end; ++it) {

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
            current->nodes_.push_back(BaseConstructor(PBranchingNode::IndexConstructor(it, end, &current)));
            break;
        }
    }

    return *current;
}


PDataNode& PBranchingNode::createDataNode(const Key& key,
                                          const void* data,
                                          Length length,
                                          DataPoolManager& dataManager) {

    Key::const_iterator dataKey = key.end();
    --dataKey;

    PBranchingNode& dataParent(getCreateBranchingNode(key.begin(), dataKey));

    std::string k = dataKey->first;
    std::string v = dataKey->second;

    AutoLock<PersistentMutex> lock(dataParent.mutex_);

    PBaseNode& newNode(*dataParent.nodes_.push_back(
                       PDataNode::BaseConstructor(PDataNode::Constructor(k, v, data, length)),
                       dataManager));
    return newNode.asDataNode();
}


// -------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace tree

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
/// @date   Sep 2016

#include "fdb5/pmem/PBaseNode.h"

#include "eckit/log/Log.h"

#include "fdb5/pmem/PBranchingNode.h"
#include "fdb5/pmem/PDataNode.h"


namespace fdb5 {
namespace pmem {

// -------------------------------------------------------------------------------------------------

PBaseNode::PBaseNode(NodeType type, const KeyType& key, const ValueType& value) :
    type_(type), idKey_(key), idValue_(value) {}


bool PBaseNode::isNull() const {
    return type_ == PBaseNode::NULL_NODE;
}

bool PBaseNode::isBranchingNode() const {
    return type_ == PBaseNode::BRANCHING_NODE;
}

bool PBaseNode::isDataNode() const {
    return type_ == PBaseNode::DATA_NODE;
}

PBranchingNode& PBaseNode::asBranchingNode() {
    ASSERT(isBranchingNode());

    return *(static_cast<PBranchingNode*>(this));
}

PDataNode& PBaseNode::asDataNode() {
    ASSERT(isDataNode());

    return *(static_cast<PDataNode*>(this));
}

bool PBaseNode::matches(const KeyType& key, const ValueType& value) const {
    return key == idKey_ && value == idValue_;
}

std::string PBaseNode::key() const {
    return idKey_;
}

std::string PBaseNode::value() const {
    return idValue_;
}

std::ostream& operator<<(std::ostream& s, const PBaseNode& n) {

    // TODO: Include the node _type_ here

    s << "Node(" << n.idKey_ << ":" << n.idValue_ << ")";
    return s;
}

// -------------------------------------------------------------------------------------------------

}  // namespace pmem
}  // namespace fdb5

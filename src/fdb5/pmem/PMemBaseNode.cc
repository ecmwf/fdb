/*
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-3.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Sep 2016

#include "eckit/log/Log.h"

#include "fdb5/pmem/PMemBaseNode.h"


namespace fdb5 {

// -------------------------------------------------------------------------------------------------

PMemBaseNode::Constructor::Constructor( PMemBaseNode::NodeType type,
                                        const KeyType& key,
                                        const ValueType& value) :
    type_(type),
    key_(key),
    value_(value) {
    eckit::Log::error() << "BaseConstructor(" << key_ << ":" << value_ << ")";
}


void PMemBaseNode::Constructor::constructBase(PMemBaseNode &object) const {
    object.type_ = type_;
    object.idKey_ = key_;
    object.idValue_ = value_;
    eckit::Log::error() << "constructBase(" << key_ << ":" << value_ << ")" << std::endl;
}

bool PMemBaseNode::isNull() const {
    return type_ == PMemBaseNode::NULL_NODE;
}

bool PMemBaseNode::isBranchingNode() const {
    return type_ == PMemBaseNode::BRANCHING_NODE;
}

bool PMemBaseNode::isDataNode() const {
    return type_ == PMemBaseNode::DATA_NODE;
}

bool PMemBaseNode::matches(const KeyType& key, const ValueType& value) const {
    return key == idKey_ && value == idValue_;
}

std::ostream& operator<< (std::ostream& s, const PMemBaseNode& n) {

    // TODO: Include the node _type_ here

    s << "Node(" << n.idKey_ << ":" << n.idValue_ << ")";
    return s;
}

// -------------------------------------------------------------------------------------------------

} // namespace tree

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
#include "eckit/io/Length.h"
#include "eckit/log/Log.h"
#include "eckit/parser/JSONDataBlob.h"
#include "eckit/types/Types.h"

#include "fdb5/database/Key.h"
#include "fdb5/pmem/PMemDataNode.h"

#include <unistd.h>

using namespace eckit;
using namespace pmem;


namespace fdb5 {

// -------------------------------------------------------------------------------------------------

PMemDataNode::Constructor::Constructor(const KeyType& key,
                                       const ValueType& value,
                                       const void* data,
                                       Length length) :
    PMemBaseNode::Constructor(DATA_NODE, key, value),
    length_(length),
    data_(data) {
    Log::error() << "Constructor(" << key << ": " << value << " -- " << length << std::endl;
}

size_t PMemDataNode::Constructor::size() const {
    return sizeof(PMemDataNode) - sizeof(PMemDataNode::data_) + length_;
}

void PMemDataNode::Constructor::make(PMemDataNode& object) const {
    constructBase(object);
    Log::info() << "Base constructed" << std::endl;

    object.length_ = length_;
    memcpy(object.data_, data_, length_);
}

/// A wrapper to allow us to pass a PMemDataNode constructor into allocate for the base class

PMemDataNode::BaseConstructor::BaseConstructor(const PMemDataNode::Constructor& ctr) :
    ctr_(ctr) {}

size_t PMemDataNode::BaseConstructor::size() const {
    return ctr_.size();
}

void PMemDataNode::BaseConstructor::make(PMemBaseNode& object) const {
    Log::error() << "Redirecting make --> PMemDataNode" << std::endl;
    ctr_.make(static_cast<PMemDataNode&>(object));
}


// -------------------------------------------------------------------------------------------------


// -------------------------------------------------------------------------------------------------

} // namespace tree

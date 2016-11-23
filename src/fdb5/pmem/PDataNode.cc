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
#include "fdb5/pmem/PDataNode.h"

#include <unistd.h>

using namespace eckit;
using namespace pmem;


namespace fdb5 {
namespace pmem {

// ---------------------------------------------------------------------------------------------------------------------

PDataNode::Constructor::Constructor(const KeyType& key,
                                    const ValueType& value,
                                    const void* data,
                                    Length length) :
    PBaseNode::Constructor(DATA_NODE, key, value),
    length_(length),
    data_(data) {
    Log::error() << "Constructor(" << key << ": " << value << " -- " << length << std::endl;
}

size_t PDataNode::Constructor::size() const {
    return sizeof(PDataNode) - sizeof(PDataNode::data_) + length_;
}

void PDataNode::Constructor::make(PDataNode& object) const {
    constructBase(object);

    object.length_ = length_;
    memcpy(object.data_, data_, length_);
}

// ---------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace tree

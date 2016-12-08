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
#include "fdb5/pmem/PMemIndex.h"
#include "fdb5/pmem/PMemFieldLocation.h"
#include "fdb5/pmem/PBranchingNode.h"

#include <unistd.h>

using namespace eckit;
using namespace pmem;


namespace fdb5 {
namespace pmem {

// ---------------------------------------------------------------------------------------------------------------------


PDataNode::PDataNode(const KeyType& key, const ValueType& value, const void* data, eckit::Length length) :
    PBaseNode(DATA_NODE, key, value),
    length_(length) {

    ::memcpy(data_, data, length_);
}


size_t PDataNode::data_size(eckit::Length length) {
    return sizeof(PDataNode) - sizeof(PDataNode::data_) + length;
}


const void* PDataNode::data() const {
    return data_;
}


eckit::Length PDataNode::length() const {
    return length_;
}


// ---------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace tree

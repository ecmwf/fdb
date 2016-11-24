/*
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-3.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Sept 2016

#include "eckit/io/DataBlob.h"
#include "eckit/io/DataHandle.h"
#include "eckit/log/Log.h"
#include "eckit/memory/ScopedPtr.h"
#include "eckit/parser/JSONDataBlob.h"
#include "eckit/types/Types.h"

#include "fdb5/config/MasterConfig.h"
#include "fdb5/pmem/PRoot.h"

#include <unistd.h>

using namespace eckit;
using namespace pmem;


namespace fdb5 {
namespace pmem {

// -------------------------------------------------------------------------------------------------


PRoot::Constructor::Constructor() {}


void PRoot::Constructor::make(PRoot& object) const {

    object.tag_ = PRootTag;
    object.version_ = PRootVersion;

    object.created_ = time(0);
    object.rootSize_ = sizeof(PRoot);

    object.createdBy_ = getuid();

    // The root node of the tree does not have an associated key/value.
    object.rootNode_.allocate_ctr(PBranchingNode::Constructor("", ""));

    object.dataPoolUUIDs_.nullify();

    // Store the currently loaded master schema, so it can be recovered later

    PathName schemaPath = MasterConfig::instance().schemaPath();
    ScopedPtr<DataHandle> schemaFile(schemaPath.fileHandle());
    Buffer buf(schemaFile->openForRead());
    schemaFile->read(buf, buf.size());

    object.schema_.allocate(static_cast<const void*>(buf), buf.size());
}

// -------------------------------------------------------------------------------------------------

/*
 * We can use whatever knowledge we have to test the validity of the structure.
 *
 * For now, we just check that the tag is set (i.e. it is initialised).
 */
bool PRoot::valid() const {

    if (tag_ != PRootTag) {
        Log::error() << "Persistent root tag does not match" << std::endl;
        return false;
    }

    if (rootSize_ != sizeof(PRoot)) {
        Log::error() << "Invalid (old) structure size for persistent root" << std::endl;
        return false;
    }

    if (version_ != PRootVersion) {
        Log::error() << "Invalid persistent root version" << std::endl;
        return false;
    }

    if (rootNode_.null() || !rootNode_.valid()) {
        Log::error() << "Inconsistent tree root node" << std::endl;
        return false;
    }

    return true;
}

const time_t& PRoot::created() const {
    return created_;
}

::pmem::PersistentPtr<PBranchingNode> PRoot::getBranchingNode(const Key& key) const {

    // Get the relevant index.

    return rootNode_->getBranchingNode(key);
}

PBranchingNode& PRoot::getCreateBranchingNode(const Key& key) {

    // Get the relevant index. If the system is open for writing then we should create
    // a new index if it doesn't exist.

    return rootNode_->getCreateBranchingNode(key);
}

void PRoot::print(std::ostream& s) const {
    s << "PRoot(0x" << this << ")";
}


// -------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

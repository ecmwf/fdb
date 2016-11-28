/*
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-3.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Sept 2016

#include "eckit/log/Log.h"
#include "eckit/memory/ScopedPtr.h"

#include "fdb5/config/MasterConfig.h"
#include "fdb5/pmem/PIndexRoot.h"

#include <unistd.h>

using namespace eckit;
using namespace pmem;


namespace fdb5 {
namespace pmem {

// -------------------------------------------------------------------------------------------------


PIndexRoot::PIndexRoot() :
    tag_(PIndexRootTag),
    version_(PIndexRootVersion),
    rootSize_(sizeof(PIndexRoot)),
    created_(time(0)),
    createdBy_(getuid()) {

    // The root node of the tree does not have an associated key/value.
    rootNode_.allocate(std::string(), std::string());

    // Store the currently loaded master schema, so it can be recovered later

    PathName schemaPath = MasterConfig::instance().schemaPath();
    ScopedPtr<DataHandle> schemaFile(schemaPath.fileHandle());
    std::string buf(static_cast<size_t>(schemaFile->openForRead()), '\0');
    schemaFile->read(&buf[0], buf.size());

    schema_.allocate(buf);
}

// -------------------------------------------------------------------------------------------------

/*
 * We can use whatever knowledge we have to test the validity of the structure.
 *
 * For now, we just check that the tag is set (i.e. it is initialised).
 */
bool PIndexRoot::valid() const {

    if (tag_ != PIndexRootTag) {
        Log::error() << "Persistent root tag does not match" << std::endl;
        return false;
    }

    if (rootSize_ != sizeof(PIndexRoot)) {
        Log::error() << "Invalid (old) structure size for persistent root" << std::endl;
        return false;
    }

    if (version_ != PIndexRootVersion) {
        Log::error() << "Invalid persistent root version" << std::endl;
        return false;
    }

    if (rootNode_.null() || !rootNode_.valid()) {
        Log::error() << "Inconsistent tree root node" << std::endl;
        return false;
    }

    if (schema_.null()) {
        Log::error() << "Schema missing from tree root node" << std::endl;
        return false;

    }

    return true;
}

const time_t& PIndexRoot::created() const {
    return created_;
}

::pmem::PersistentPtr<PBranchingNode> PIndexRoot::getBranchingNode(const Key& key) const {

    // Get the relevant index.

    return rootNode_->getBranchingNode(key);
}

PBranchingNode& PIndexRoot::getCreateBranchingNode(const Key& key) {

    // Get the relevant index. If the system is open for writing then we should create
    // a new index if it doesn't exist.

    return rootNode_->getCreateBranchingNode(key);
}

const ::pmem::PersistentString& PIndexRoot::schema() const {
    return *schema_;
}

void PIndexRoot::print(std::ostream& s) const {
    s << "PIndexRoot(0x" << this << ")";
}


// -------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

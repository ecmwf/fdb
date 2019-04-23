/*
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-3.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @author Tiago Quintino
/// @date   Sept 2016

#include <unistd.h>

#include "eckit/log/Log.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/pmem/PIndexRoot.h"
#include "fdb5/pmem/MemoryBufferStream.h"
#include "fdb5/config/Config.h"

#include "pmem/PoolRegistry.h"

using namespace eckit;
using namespace pmem;


namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------

PIndexRoot::PIndexRoot(const PersistentPtr<PersistentBuffer>& key,
                       const PersistentPtr<PersistentString>& schema,
                       const PersistentPtr<PBranchingNode>& rootNode) :
    tag_(PIndexRootTag),
    version_(PIndexRootVersion),
    rootSize_(sizeof(PIndexRoot)),
    created_(time(0)),
    createdBy_(getuid()),
    rootNode_(rootNode),
    schema_(schema),
    dbKey_(key) {

    dataPoolUUIDs_.nullify();
}


void PIndexRoot::build(PersistentPtr<PIndexRoot>& ptr, const Key& dbKey, const eckit::PathName& schema) {

    PersistentPool& pool(PoolRegistry::instance().poolFromPointer(&ptr));

    // The root node of the tree does not have an associated key/value.

    PersistentPtr<PBranchingNode> rootNode = pool.allocate<PBranchingNode>(std::string(), std::string());

    // Store the currently loaded master schema, so it can be recovered later

    eckit::PathName schemaPath(schema.exists() ? schema : LibFdb5::instance().defaultConfig().schemaPath());
    std::unique_ptr<DataHandle> schemaFile(schemaPath.fileHandle());
    std::string buf(static_cast<size_t>(schemaFile->openForRead()), '\0');
    schemaFile->read(&buf[0], buf.size());

    PersistentPtr<PersistentString> schemaObj = pool.allocate<PersistentString>(buf);

    // Store the current DB key

    MemoryBufferStream s;
    s << dbKey;
    const void* key_data = s.buffer();

    PersistentPtr<PersistentBuffer> key = pool.allocate<PersistentBuffer>(key_data, s.position());

    ptr.allocate(key, schemaObj, rootNode);
}

//----------------------------------------------------------------------------------------------------------------------

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

    if (dbKey_.null()) {
        Log::error() << "(Root) database key missing from tree root node" << std::endl;
        return false;
    }

    return true;
}

const time_t& PIndexRoot::created() const {
    return created_;
}

unsigned short int PIndexRoot::version() const {
    return version_;
}

long PIndexRoot::uid() const {
    return createdBy_;
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

void PIndexRoot::visitLeaves(EntryVisitor& visitor, DataPoolManager& mgr, const Schema& schema) const {

    std::vector<Key> keys;
    keys.push_back(databaseKey());
    keys.push_back(Key());
    return rootNode_->visitLeaves(visitor, mgr, keys, 1);
}

const ::pmem::PersistentString& PIndexRoot::schema() const {
    return *schema_;
}

const ::pmem::PersistentPODVector<uint64_t>& PIndexRoot::dataPoolUUIDs() const {
    return dataPoolUUIDs_;
}

Key PIndexRoot::databaseKey() const {

    ASSERT(!dbKey_.null());
    const PersistentBuffer& buf(*dbKey_);
    MemoryStream s(buf.data(), buf.size());

    Key k(s);
    return k;
}

void PIndexRoot::print(std::ostream& s) const {
    s << "PIndexRoot(0x" << this << ")";
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

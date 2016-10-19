/*
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-3.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Feb 2016


#include "eckit/log/Log.h"
#include "eckit/log/TimeStamp.h"

#include "pmem/PersistentPtr.h"
#include "pmem/Exceptions.h"

#include "fdb5/pmem/PMemPool.h"
#include "fdb5/pmem/PMemRoot.h"
#include "fdb5/pmem/PMemBaseNode.h"
#include "fdb5/pmem/PMemBranchingNode.h"

using namespace eckit;
using namespace pmem;


template<> uint64_t pmem::PersistentPtr<fdb5::PMemRoot>::type_id = POBJ_ROOT_TYPE_NUM;

template<> uint64_t pmem::PersistentPtr<fdb5::PMemBaseNode>::type_id = 1;
template<> uint64_t pmem::PersistentPtr<fdb5::PMemBranchingNode>::type_id = 2;
template<> uint64_t pmem::PersistentPtr<fdb5::PMemDataNode>::type_id = 3;
template<> uint64_t pmem::PersistentPtr<pmem::PersistentVectorData<fdb5::PMemBaseNode> >::type_id = 4;
template<> uint64_t pmem::PersistentPtr<pmem::PersistentPODVectorData<uint64_t> >::type_id = 5;


namespace fdb5 {

// -------------------------------------------------------------------------------------------------

PMemPool::PMemPool(const PathName& path, const std::string& name) :
    PersistentPool(path, name) {

    ASSERT(root()->valid());

    Log::info() << "Opened persistent pool created at: " << TimeStamp(root()->created()) << std::endl;
}




PMemPool::PMemPool(const PathName& path, const size_t size, const std::string& name,
                   const AtomicConstructor<PMemRoot>& constructor) :
    PersistentPool(path, size, name, constructor) {}


PMemPool::~PMemPool() {}


/// Open an existing persistent pool, if it exists. If it does _not_ exist, then create it.
/// If open/create fail for other reasons, then the appropriate error is thrown.
///
/// @arg path - Specifies the directory to open, rather than the pool size itself.
PMemPool* PMemPool::obtain(const PathName &poolDir, const size_t size) {

    // The pool must exist as a file within a directory.
    // n.b. PathName::mkdir uses mkdir_if_not_exists internally, so works OK if another process gets ahead.
    if (!poolDir.exists())
        poolDir.mkdir();
    ASSERT(poolDir.isDir());

    PathName masterPath = poolDir / "master";
    PMemPool* pool = 0;

    try {

        Log::error() << "Opening.. " << std::endl;
        pool = new PMemPool(masterPath, "pmem-pool");

    } catch (PersistentOpenError& e) {
        if (e.errno_ == ENOENT)
            pool = new PMemPool(masterPath, size, "pmem-pool", PMemRoot::Constructor());
        else
            throw;
    }

    ASSERT(pool != 0);
    return pool;
}


PersistentPtr<PMemRoot> PMemPool::root() const {

    PersistentPtr<PMemRoot> rt = getRoot<PMemRoot>();
    ASSERT(rt.valid());

    return rt;
}

// -------------------------------------------------------------------------------------------------

} // namespace tree

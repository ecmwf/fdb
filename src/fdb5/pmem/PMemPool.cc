/*
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-3.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Feb 2016
///
#include "eckit/log/Log.h"

#include "pmem/PersistentPtr.h"
#include "pmem/Exceptions.h"

#include "fdb5/pmem/PMemPool.h"
#include "fdb5/pmem/PMemRoot.h"

using namespace eckit;
using namespace pmem;


template<> uint64_t pmem::PersistentPtr<fdb5::PMemRoot>::type_id = POBJ_ROOT_TYPE_NUM;


namespace fdb5 {

// -------------------------------------------------------------------------------------------------

PMemPool::PMemPool(const eckit::PathName& path, const std::string& name) :
    PersistentPool(path, name) {}


PMemPool::PMemPool(const eckit::PathName& path, const size_t size, const std::string& name,
                   const AtomicConstructorBase& constructor) :
    PersistentPool(path, size, name, constructor) {}


PMemPool::~PMemPool() {}


PMemPool* PMemPool::obtain(const eckit::PathName &path, const size_t size) {

    // Open an existing persistent pool, if it exists. If it does _not_ exist, then create it.
    // If open/create fail for other reasons, then the appropriate error is thrown.

    PMemPool* pool = 0;

    try {

        Log::error() << "Opening.. " << std::endl;
        pool = new PMemPool(path, "pmem-pool");

    } catch (PersistentOpenError& e) {
        if (e.errno_ == ENOENT)
            pool = new PMemPool(path, size, "pmem-pool", PMemRoot::Constructor());
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

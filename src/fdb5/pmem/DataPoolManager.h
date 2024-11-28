/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
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


#ifndef fdb5_pmem_DataPoolManager_H
#define fdb5_pmem_DataPoolManager_H

#include <cstdint>
#include <iosfwd>
#include <map>

#include "eckit/memory/NonCopyable.h"
#include "eckit/filesystem/PathName.h"

#include "pmem/AtomicConstructor.h"
#include "pmem/PersistentPtr.h"

#include "fdb5/pmem/DataPool.h"



namespace eckit {
    class PathName;
}


namespace fdb5 {
namespace pmem {

class PIndexRoot;

// ---------------------------------------------------------------------------------------------------------------------

/*
 * This class needs to:
 *
 * - Open data pools for reading when they are required. Given all the memory is there. Maintain
 *   the mapping with uuids.
 * - When a pool is filled up (writing), open a new one
 * - Generate the pool filenames based off the filename of the DB pool, + a counting integer
 * - Manage atomic allocations, with retries.
 */

class DataPoolManager : private eckit::NonCopyable {

public: // methods

    DataPoolManager(const eckit::PathName& poolDir, PIndexRoot& masterRoot, uint64_t rootUUID);

    ~DataPoolManager();

    /// Allocate data into the currently active pool
    template <typename T>
    void allocate(::pmem::PersistentPtr<T>& ptr, const ::pmem::AtomicConstructor<T>& ctr);

    /// Ensure that the pool associated with the specified UUID is loaded. If it is not, then load
    /// it. If it is unavailable, then throw an error.
    void ensurePoolLoaded(uint64_t uuid);

    /// For output/debugging purposes
    eckit::PathName dataPoolPath(uint64_t uuid);

    DataPool& getPool(uint64_t uuid);

    /// How many data pools are under management?
    size_t dataPoolsCount() const;

    /// The total size of the data pools (does not include the master pool).
    size_t dataSize();

    std::vector<eckit::PathName> dataPoolPaths();

protected: // methods

    /// Obtain the current pool for writing. If no pool is opened, then open/create the latest one.
    DataPool& currentWritePool();

    /// The current pool is full! Finalise it, and create a new one. Check for the obvious complications (e.g. another
    /// process, or thread, has already created a new pool, or the pool being used is already out of date).
    void invalidateCurrentPool(DataPool& pool);

    void print(std::ostream& out) const;

    const std::map<uint64_t, DataPool*>& pools() const;

private: // members

    eckit::PathName poolDir_;

    /// A mapping of pools' UUIDs to the opened pool objects.
    std::map<uint64_t, DataPool*> pools_;

    PIndexRoot& masterRoot_;
    uint64_t masterUUID_;

    DataPool* currentPool_;

private: // friends

    friend std::ostream& operator<<(std::ostream& s,const DataPoolManager& p) { p.print(s); return s; }
};


// ---------------------------------------------------------------------------------------------------------------------

// TODO: Deprecate and remove.
template <typename T>
void DataPoolManager::allocate(::pmem::PersistentPtr<T>& ptr, const ::pmem::AtomicConstructor<T>& ctr) {

    // n.b. Remember to lock the PMemRoot whilst updating the list of pools.

    while (true) {

        DataPool& pool(currentWritePool());

        try {

            ptr.allocate_ctr(pool, ctr);
            break;

        } catch (::pmem::AtomicConstructorBase::AllocationError& e) {
            // TODO: Check errno
            // If the allocation fails due to lack of space, this is fine. We just need to retry.
        }

        invalidateCurrentPool(pool);
    };
}

} // namespace pmem
} // namespace fdb5

#endif // fdb5_pmem_PMemDataPoolManager_H

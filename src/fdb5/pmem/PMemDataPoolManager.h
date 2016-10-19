/*
 * (C) Copyright 1996-2015 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Sep 2016


#ifndef fdb5_pmem_PMemDataPoolManager_H
#define fdb5_pmem_PMemDataPoolManager_H

#include "eckit/memory/NonCopyable.h"

#include "pmem/AtomicConstructor.h"
#include "pmem/PersistentPtr.h"

#include "fdb5/pmem/PMemDataPool.h"

#include <stdint.h>
#include <iosfwd>
#include <map>


namespace eckit {
    class PathName;
}


namespace fdb5 {

class PMemRoot;

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

class PMemDataPoolManager : private eckit::NonCopyable {

public: // methods

    PMemDataPoolManager(const eckit::PathName& poolDir, PMemRoot& masterRoot);
    ~PMemDataPoolManager();

    /// Allocate data into the currently active pool
    template <typename T>
    void allocate(pmem::PersistentPtr<T>& ptr, const pmem::AtomicConstructor<T>& ctr);

private: // methods

    /// Obtain the current pool for writing. If no pool is opened, then open/create the latest one.
    PMemDataPool& currentWritePool();

    /// The current pool is full! Finalise it, and create a new one. Check for the obvious complications (e.g. another
    /// process, or thread, has already created a new pool, or the pool being used is already out of date).
    void invalidateCurrentPool(PMemDataPool& pool);

    virtual void print(std::ostream& out) const;

private: // members

    eckit::PathName poolDir_;

    /// A mapping of pools' UUIDs to the opened pool objects.
    std::map<uint64_t, PMemDataPool*> pools_;

    PMemRoot& masterRoot_;

    PMemDataPool* currentPool_;

private: // friends

    friend std::ostream& operator<<(std::ostream& s,const PMemDataPoolManager& p) { p.print(s); return s; }
};


// ---------------------------------------------------------------------------------------------------------------------

template <typename T>
void PMemDataPoolManager::allocate(pmem::PersistentPtr<T>& ptr, const pmem::AtomicConstructor<T>& ctr) {

    // n.b. Remember to lock the PMemRoot whilst updating the list of pools.

    while (true) {

        PMemDataPool& pool(currentWritePool());

        try {

            ptr.allocate(pool, ctr);
            break;

        } catch (pmem::AtomicConstructorBase::AllocationError& e) {

            // TODO: Check errno
            // If the allocation fails due to lack of space, this is fine. We just need to retry.
        }

        invalidateCurrentPool(pool);
    };
}

} // namespace fdb5

#endif // fdb5_pmem_PMemDataPoolManager_H

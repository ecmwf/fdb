/*
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-3.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Sept 2016


#include "eckit/filesystem/PathName.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/config/Resource.h"

#include "pmem/PersistentMutex.h"

#include "fdb5/pmem/PMemDataPoolManager.h"
#include "fdb5/pmem/PMemDataPool.h"
#include "fdb5/pmem/PMemPool.h"
#include "fdb5/pmem/PMemRoot.h"

using namespace eckit;
using namespace pmem;


namespace fdb5 {

// ---------------------------------------------------------------------------------------------------------------------

PMemDataPoolManager::PMemDataPoolManager(const PathName& poolDir, PMemRoot& masterRoot) :
    poolDir_(poolDir),
    masterRoot_(masterRoot),
    currentPool_(0) {

    Log::error() << "Construct" << *this << std::endl;
}


PMemDataPoolManager::~PMemDataPoolManager() {

    // Clean up the pools

    std::map<uint64_t, PMemDataPool*>::iterator it = pools_.begin();

    for (; it != pools_.end(); ++it) {
        delete it->second;
    }
}



PMemDataPool& PMemDataPoolManager::currentWritePool() {

    Log::error() << "[" << *this << "]: " << "Requesting current pool" << std::endl;

    if (currentPool_ == 0) {

        // If there isn't a currently active pool, we are potentially going to be modifying the root object,
        // so lock things.

        Log::error() << "locking... " << std::endl;
        AutoLock<PersistentMutex> lock(masterRoot_.mutex_);

        Log::error() << "Master root: " << masterRoot_ << std::endl;
        Log::error() << "Master root uuids: " << masterRoot_.dataPoolUUIDs_ << std::endl;

        size_t pool_count = masterRoot_.dataPoolUUIDs_.size();

        // If there are any pools in the pool list, open the last one. If this is not finalised, then use it.

        if (pool_count > 0) {

            // TODO: Given index, get UUID
            // TODO: Given UUID, have function that checks if it is in the pools_ list, and if not, open it.
            size_t idx = pool_count - 1;
            Log::error() << "[" << *this << "]: " << "Opening pool index: " << idx << std::endl;

            PMemDataPool* pool = new PMemDataPool(poolDir_, idx);

            size_t uuid = pool->uuid();
            ASSERT(pools_.find(uuid) == pools_.end());
            pools_[uuid] = pool;

            if (!pool->finalised())
                currentPool_ = pool;
        }

        // If we still don't have a workable pool, we need to open a new one.
        if (currentPool_ == 0) {

            Log::error() << "Create new..." << std::endl;
            size_t idx = pool_count;
            Log::error() << "Creating a new data pool with index: " << idx << std::endl;
            currentPool_ = new PMemDataPool(poolDir_, idx, Resource<size_t>("fdbPMemDataPoolSize", 1024 * 1024 * 1024));

            // Add pool to the list of opened pools

            size_t uuid = currentPool_->uuid();
            ASSERT(pools_.find(uuid) == pools_.end());
            pools_[uuid] = currentPool_;

            masterRoot_.dataPoolUUIDs_.push_back(uuid);
        }
    }

    return *currentPool_;
}


void PMemDataPoolManager::invalidateCurrentPool(PMemDataPool& pool) {

    // If the supplied pool does not equal currentPool_, then another thread/process has already done this.
    if (&pool != currentPool_)
        return;

    // We don't worry too much about other threads/processors. They will also hit out-of-space allocation errors.
    // This finalisation is only for informational purposes.

    currentPool_->finalise();
    currentPool_ = 0;
}

void PMemDataPoolManager::print(std::ostream& s) const {
    s << "PMemDataPoolManager(" << poolDir_ << ")";
}


// ---------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

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

#include "fdb5/pmem/DataPoolManager.h"
#include "fdb5/pmem/DataPool.h"
#include "fdb5/pmem/Pool.h"
#include "fdb5/pmem/PIndexRoot.h"

using namespace eckit;
using namespace pmem;


namespace fdb5 {
namespace pmem {

// ---------------------------------------------------------------------------------------------------------------------

DataPoolManager::DataPoolManager(const PathName& poolDir, PIndexRoot& masterRoot, uint64_t rootUUID) :
    poolDir_(poolDir),
    masterRoot_(masterRoot),
    masterUUID_(rootUUID),
    currentPool_(0) {}


DataPoolManager::~DataPoolManager() {

    // Clean up the pools

    std::map<uint64_t, DataPool*>::iterator it = pools_.begin();

    for (; it != pools_.end(); ++it) {
        delete it->second;
    }
}


DataPool& DataPoolManager::currentWritePool() {

    Log::error() << "[" << *this << "]: " << "Requesting current pool" << std::endl;

    if (currentPool_ == 0) {

        // If there isn't a currently active pool, we are potentially going to be modifying the root object,
        // so lock things.

        Log::error() << "locking... " << std::endl;
        AutoLock<PersistentMutex> lock(masterRoot_.mutex_);

        Log::error() << "Master root: " << masterRoot_ << std::endl;
        Log::error() << "Master root uuids: " << masterRoot_.dataPoolUUIDs_ << std::endl;

        size_t pool_count = masterRoot_.dataPoolUUIDs_.size();
        Log::error() << "Pool count: " << pool_count << std::endl;

        // If there are any pools in the pool list, open the last one. If this is not finalised, then use it.

        if (pool_count > 0) {

            // TODO: Given index, get UUID
            // TODO: Given UUID, have function that checks if it is in the pools_ list, and if not, open it.
            size_t idx = pool_count - 1;
            size_t uuid = masterRoot_.dataPoolUUIDs_[idx];
            DataPool* pool;

            if (pools_.find(uuid) == pools_.end()) {

                Log::error() << "[" << *this << "]: " << "Opening pool index: " << idx << std::endl;
                pool = new DataPool(poolDir_, idx);

                uuid = pool->uuid();
                pools_[uuid] = pool;
            } else {
                pool = pools_[uuid];
            }

            if (!pool->finalised())
                currentPool_ = pool;
        }

        // If we still don't have a workable pool, we need to open a new one.
        if (currentPool_ == 0) {

            Log::error() << "Create new..." << std::endl;
            size_t idx = pool_count;
            Log::error() << "Creating a new data pool with index: " << idx << std::endl;
            currentPool_ = new DataPool(poolDir_, idx, Resource<size_t>("fdbPMemDataPoolSize", 1024 * 1024 * 1024));

            // Add pool to the list of opened pools

            size_t uuid = currentPool_->uuid();
            ASSERT(pools_.find(uuid) == pools_.end());
            pools_[uuid] = currentPool_;

            masterRoot_.dataPoolUUIDs_.push_back(uuid);
        }
    }

    return *currentPool_;
}


void DataPoolManager::invalidateCurrentPool(DataPool& pool) {

    // If the supplied pool does not equal currentPool_, then another thread/process has already done this.
    if (&pool != currentPool_)
        return;

    // We don't worry too much about other threads/processors. They will also hit out-of-space allocation errors.
    // This finalisation is only for informational purposes.

    Log::error() << "[" << *this << "]: " << "Finalising current pool" << std::endl;

    currentPool_->finalise();
    currentPool_ = 0;
}


void DataPoolManager::ensurePoolLoaded(uint64_t uuid) {

    if (uuid != masterUUID_ && pools_.find(uuid) == pools_.end()) {

        Log::info() << "Data pool with UUID=" << uuid << " not yet opened" << std::endl;

        size_t pool_count = masterRoot_.dataPoolUUIDs_.size();

        for (size_t idx = 0; idx < pool_count; idx++) {
            if (uuid == masterRoot_.dataPoolUUIDs_[idx]) {
                Log::info() << "Opening data pool, UUID=" << uuid << ", index=" << idx << std::endl;
                pools_[uuid] = new DataPool(poolDir_, idx);
                return;
            }
        }

        // If none are found, then we have an issue.

        std::stringstream err;
        err << "Data pool " << uuid << " not found";
        throw SeriousBug(err.str(), Here());
    }
}


void DataPoolManager::print(std::ostream& s) const {
    s << "PMemDataPoolManager(" << poolDir_ << ")";
}


// ---------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

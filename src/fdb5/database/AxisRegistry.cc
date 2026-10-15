// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/database/AxisRegistry.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/Mutex.h"

#include <memory>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

AxisRegistry& AxisRegistry::instance() {
    static AxisRegistry axisregistry;
    return axisregistry;
}

void AxisRegistry::release(const keyword_t& keyword, std::shared_ptr<axis_t>& ptr) {

    // use_count() can change concurrently, so it must be re-checked under the lock
    // this is only an optimisation to avoid locking
    if (ptr.use_count() > 2) {
        return;
    }

    eckit::AutoLock lock(mutex_);

    if (ptr.use_count() != 2) {
        return;
    }

    axis_map_t::iterator it = axes_.find(keyword);
    ASSERT(it != axes_.end());

    it->second.erase(ptr);

    ASSERT(ptr.use_count() == 1);

    if (it->second.empty()) {
        axes_.erase(it);
    }
}

void AxisRegistry::deduplicate(const keyword_t& keyword, std::shared_ptr<axis_t>& ptr) {

    eckit::AutoLock<eckit::Mutex> lock(mutex_);

    //    static std::size_t dedups = 0;

    axis_store_t& axis = axes_[keyword];
    axis_store_t::iterator it = axis.find(ptr);
    if (it == axis.end()) {
        axis.insert(ptr);
    }
    else {
        //        dedups++;
        //        LOG_DEBUG_LIB(LibFdb5) << dedups << " deduped axis [" << *ptr << "]" << std::endl;
        ptr = *it;
    }
}

}  // namespace fdb5

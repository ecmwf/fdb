/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <sstream>

#include "eckit/log/Log.h"
#include "eckit/thread/AutoLock.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/AxisRegistry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

AxisRegistry& AxisRegistry::instance() {
    static AxisRegistry axisregistry;
    return axisregistry;
}

void AxisRegistry::release(const keyword_t& keyword, std::shared_ptr<axis_t>& ptr) {

    if (ptr.use_count() != 2) {
        return;
    }

    eckit::AutoLock<eckit::Mutex> lock(mutex_);

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

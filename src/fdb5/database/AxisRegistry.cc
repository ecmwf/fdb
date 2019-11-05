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

#include "eckit/thread/AutoLock.h"

#include "fdb5/database/AxisRegistry.h"

namespace fdb5 {

AxisRegistry& AxisRegistry::instance() {
    static AxisRegistry axisregistry;
    return axisregistry;
}

AxisRegistry::axis_key_t AxisRegistry::axisToKeyword(const axis_t& axis) {
    std::ostringstream oss;
    char sep = '/';
    for (axis_t::const_iterator it = axis.begin(); it != axis.end(); ++it) {
        oss << *it << sep;
    }
    return oss.str();
}

void AxisRegistry::release(std::string keyword, std::shared_ptr<axis_t>& ptr) {
    ASSERT(ptr.use_count() >= 2);

    if (ptr.use_count() > 2)
        return;

    eckit::AutoLock<eckit::Mutex> lock(mutex_);

    axis_map_t::iterator it = axes_.find(keyword);
    ASSERT(it != axes_.end());

    AxisRegistry::axis_key_t key = axisToKeyword(*ptr);
    it->second.erase(key);

    if (it->second.empty())
        axes_.erase(it);
}

void AxisRegistry::deduplicate(std::string keyword, std::shared_ptr<axis_t>& ptr) {
    eckit::AutoLock<eckit::Mutex> lock(mutex_);

    axis_store_t& axis = axes_[keyword];
    AxisRegistry::axis_key_t key = axisToKeyword(*ptr);
    axis_store_t::iterator it = axis.find(key);
    if (it == axis.end())
        axis.insert({key, ptr});
    else
        ptr = it->second;
}

}


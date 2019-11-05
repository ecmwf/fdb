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

AxisRegistry::axis_key_t AxisRegistry::get_key(const axis_t &axis) {
    std::ostringstream oss;
    char sep[] = {0, 0};
    for (axis_t::const_iterator it = axis.begin(); it != axis.end(); ++it) {
        oss << sep << *it;
        sep[0] = '/';
    }
    return oss.str();
}

void AxisRegistry::release(std::string key, std::shared_ptr<axis_t>& ptr) {
    ASSERT(ptr.use_count() >= 2);

    if (ptr.use_count() > 2)
        return;

    eckit::AutoLock<eckit::Mutex> lock(instance().mutex_);

    axis_map_t::iterator it = instance().axes_.find(key);
    ASSERT(it != instance().axes_.end());

    AxisRegistry::axis_key_t data_key = get_key(*ptr);
    it->second.erase(data_key);

    if (it->second.empty())
        instance().axes_.erase(it);
}

void AxisRegistry::deduplicate(std::string key, std::shared_ptr<axis_t>& ptr) {
    eckit::AutoLock<eckit::Mutex> lock(instance().mutex_);

    axis_store_t& axis = instance().axes_[key];
    AxisRegistry::axis_key_t data_key = get_key(*ptr);
    axis_store_t::iterator it = axis.find(data_key);
    if (it == axis.end())
        axis.insert({data_key, ptr});
    else
        ptr = it->second;
}

}


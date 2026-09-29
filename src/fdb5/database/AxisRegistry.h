/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   AxisRegistry.h
/// @author Olivier Iffrig
/// @author Tiago Quintino
/// @date   Nov 2019

#ifndef fdb5_AxisRegistry_H
#define fdb5_AxisRegistry_H

#include <functional>
#include <map>
#include <memory>
#include <unordered_set>

#include "eckit/container/DenseSet.h"
#include "eckit/thread/Mutex.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class AxisRegistry {
public:  // types

    typedef std::string keyword_t;
    typedef eckit::DenseSet<std::string> axis_t;
    typedef std::shared_ptr<axis_t> ptr_axis_t;

    struct HashDenseSet {
        std::size_t operator()(ptr_axis_t const& p) const noexcept {
            std::size_t h = 0;
            for (const auto& s : *p) {
                // this hash combine is inspired in the boost::hash_combine
                // 0x9e3779b9 is the reciprocal of the golden ratio to ensure random bit distribution
                h ^= std::hash<std::string>{}(s) + 0x9e3779b9 + (h << 6) + (h >> 2);
            }
            return h;
        }
    };

    struct EqualsDenseSet {
        bool operator()(ptr_axis_t const& left, ptr_axis_t const& right) const noexcept { return *left == *right; }
    };

private:  // types

    typedef std::string axis_key_t;
    typedef std::unordered_set<ptr_axis_t, HashDenseSet, EqualsDenseSet> axis_store_t;
    typedef std::map<keyword_t, axis_store_t> axis_map_t;

public:  // methods

    static AxisRegistry& instance();

    void deduplicate(const keyword_t& key, std::shared_ptr<axis_t>& ptr);
    void release(const keyword_t& key, std::shared_ptr<axis_t>& ptr);

private:  // members

    axis_map_t axes_;

    mutable eckit::Mutex mutex_;
};

}  // namespace fdb5

#endif

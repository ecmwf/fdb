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

#include <map>
#include <memory>
#include <unordered_map>

#include "eckit/container/DenseSet.h"
#include "eckit/thread/Mutex.h"

namespace fdb5 {

class AxisRegistry {
public: // types

    typedef eckit::DenseSet<std::string> axis_t;

private: // types

    typedef std::string axis_key_t;
    typedef std::unordered_map<axis_key_t, std::shared_ptr<axis_t> > axis_store_t;
    typedef std::map<std::string, axis_store_t> axis_map_t;

public: // methods

    static AxisRegistry& instance();

    void deduplicate(std::string key, std::shared_ptr<axis_t>& ptr);
    void release(std::string key, std::shared_ptr<axis_t>& ptr);

private: // methods

    static axis_key_t axisToKeyword(const axis_t &axis);

private: // members

    axis_map_t axes_;

    mutable eckit::Mutex mutex_;
};

}

#endif


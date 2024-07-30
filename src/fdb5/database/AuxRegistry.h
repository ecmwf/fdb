
/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


/// @author Christopher Bradley
/// @date   July 2024

#pragma once

#include <set>
#include <string>
#include <mutex>

#include "eckit/memory/NonCopyable.h"

namespace fdb5 {

// Holds identifiers of auxiliary objects created by external tools or plugins.
// Specifics depend on backend. E.g., TocStore will convert entries to file extensions.
class AuxRegistry : private eckit::NonCopyable {

public: // methods

    static AuxRegistry& instance();

    void enregister(const std::string uri);
    void deregister(const std::string uri);

    const std::set<std::string>& list() { return list_; }

private: // methods

    AuxRegistry();
    ~AuxRegistry() = default;

private: // members

    std::mutex mutex_;
    std::set<std::string> list_;

};

} // namespace fdb5
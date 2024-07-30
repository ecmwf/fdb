
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

#include "eckit/config/Resource.h"

#include "fdb5/database/AuxRegistry.h"

namespace fdb5 {

AuxRegistry& AuxRegistry::instance() {
    static AuxRegistry instance;
    return instance;
}

AuxRegistry::AuxRegistry() : 
    list_(eckit::Resource<std::set<std::string>>("$FDB_AUX_EXTENSIONS;fdbAuxExtensions", {"gribjump"})) {}

void AuxRegistry::enregister(const std::string name) {
    std::lock_guard<std::mutex> lock(mutex_);
    list_.insert(name);
}

void AuxRegistry::deregister(const std::string name) {
    std::lock_guard<std::mutex> lock(mutex_);
    list_.erase(name);
}

} // namespace fdb5
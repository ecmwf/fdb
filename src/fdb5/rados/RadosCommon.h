/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Nicolau Manubens
/// @date   Feb 2024

#pragma once

#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"

#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "eckit/io/rados/RadosKeyValue.h"

#include <optional>
#include <string>
#include <vector>

namespace fdb5 {

struct RadosSpace {
    std::string pool;
    std::string rootNamespace;
    std::string namespacePrefix;

    std::string databaseNamespace(const Key& key) const;
};

// Reads the persisted `key` entry from a RADOS DB KV and deserialises it into an fdb5::Key.
// Throws eckit::RadosEntityNotFoundException if the DB KV or the `key` entry is missing.
fdb5::Key read_db_key(const eckit::RadosKeyValue& db_kv);

// RADOS space is selected from the sole root of the first matching `spaces[]` entry.
RadosSpace rados_space(const Config&, const Key&);
RadosSpace rados_space(const Config&, const eckit::URI&);
std::vector<RadosSpace> rados_spaces(const Config&);

class RadosCommon {

public:  // methods

    RadosCommon(const Config&, const std::string& component, const Key&);
    RadosCommon(const Config&, const std::string& component, const eckit::URI&);

private:  // methods

    void readConfig(const Config& config, const std::string& component);

protected:  // members

    std::string pool_;
    std::string db_namespace_;

    std::optional<eckit::RadosKeyValue> root_kv_;
    std::optional<eckit::RadosKeyValue> db_kv_;

    eckit::Length maxPartSize_;
};

}  // namespace fdb5

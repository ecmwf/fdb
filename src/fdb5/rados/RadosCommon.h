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
#include "fdb5/fdb5_config.h"

#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "eckit/io/rados/RadosKeyValue.h"

#include <optional>
#include <string>

namespace fdb5 {

/// @note: maximum length (in bytes) of the serialised blobs exchanged with Rados key-values
///   (index/field/db locations, serialised keys, axis names).
/// @todo: make configurable (the call sites currently carry a "take from config" note).
constexpr long RADOS_MAX_SERIALISED_LEN = 512;

class RadosCommon {

public:  // methods

    RadosCommon(const Config&, const std::string& component, const Key&);
    RadosCommon(const Config&, const std::string& component, const eckit::URI&);

private:  // methods

    void readConfig(const Config& config, const std::string& component, bool readPool);

protected:  // members

    std::string pool_;
    std::string root_namespace_;
    std::string db_namespace_;

    std::optional<eckit::RadosKeyValue> root_kv_;
    std::optional<eckit::RadosKeyValue> db_kv_;

    eckit::Length maxPartSize_;

private:  // members

    std::string nspace_prefix_;
};

}  // namespace fdb5

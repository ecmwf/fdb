/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   DaosCommon.h
/// @author Nicolau Manubens
/// @date   June 2023

#pragma once

#include "eckit/filesystem/URI.h"

#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"

#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosOID.h"

namespace fdb5 {

class DaosCommon {

public:  // methods

    DaosCommon(const fdb5::Config&, const std::string& component, const fdb5::Key&);
    DaosCommon(const fdb5::Config&, const std::string& component, const eckit::URI&);

    const fdb5::DaosKeyValueName& rootKeyValue() const { return root_kv_.value(); }
    const fdb5::DaosKeyValueName& dbKeyValue() const { return db_kv_.value(); }

private:  // methods

    void readConfig(const fdb5::Config&, const std::string& component, bool readPool);

protected:  // members

    std::string component_;

    std::string pool_;
    std::string root_cont_;
    std::string db_cont_;

    eckit::Optional<fdb5::DaosKeyValueName> root_kv_;
    eckit::Optional<fdb5::DaosKeyValueName> db_kv_;

    fdb5::DaosOID main_kv_{0, 0, DAOS_OT_KV_HASHED, OC_S1};       /// @todo: take oclass from config
    fdb5::DaosOID catalogue_kv_{0, 0, DAOS_OT_KV_HASHED, OC_S1};  /// @todo: take oclass from config
};

}  // namespace fdb5
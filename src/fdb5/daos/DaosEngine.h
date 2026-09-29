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
/// @date Feb 2022

#pragma once

#include <optional>

#include "eckit/config/LocalConfiguration.h"

#include "fdb5/database/Engine.h"

#include "fdb5/daos/DaosOID.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosEngine : public fdb5::Engine {

public:  // methods

    DaosEngine() {};

    static const char* typeName() { return "daos"; }

protected:  // methods

    std::string name() const override;

    std::string dbType() const override { NOTIMP; };

    eckit::URI location(const Key& key, const Config& config) const override { NOTIMP; };

    bool canHandle(const eckit::URI&, const Config&) const override;

    std::vector<eckit::URI> visitableLocations(const Key& key, const Config& config) const override;
    std::vector<eckit::URI> visitableLocations(const metkit::mars::MarsRequest& rq,
                                               const Config& config) const override;

    void print(std::ostream& out) const override { NOTIMP; };

private:  // methods

    void configureDaos(const Config&) const;

private:  // members

    mutable std::optional<eckit::LocalConfiguration> daos_config_;
    fdb5::DaosOID catalogue_kv_{0, 0, DAOS_OT_KV_HASHED, OC_S1};  // take oclass from config
};

//----------------------------------------------------------------------------------------------------------------------


}  // namespace fdb5

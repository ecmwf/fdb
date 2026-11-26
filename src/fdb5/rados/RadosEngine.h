// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Nicolau Manubens
/// @author Metin Cakircali
/// @date Jun 2024

#pragma once

#include "fdb5/database/Engine.h"

#include "metkit/mars/MarsRequest.h"

#include "eckit/filesystem/URI.h"

#include <functional>
#include <ostream>
#include <string>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class RadosEngine : public Engine {

public:  // methods

    RadosEngine() = default;

    static const char* typeName() { return "rados"; }

protected:  // methods

    std::string name() const override;

    std::string dbType() const override { return typeName(); };

    eckit::URI location(const Key& key, const Config& config) const override;

    bool canHandle(const eckit::URI& uri, const Config& config) const override;

    std::vector<eckit::URI> visitableLocations(const Key& key, const Config& config) const override;
    std::vector<eckit::URI> visitableLocations(const metkit::mars::MarsRequest& rq,
                                               const Config& config) const override;

    void print(std::ostream& out) const override { out << "RadosEngine"; }

private:  // methods

    std::vector<eckit::URI> visitableLocations(const std::function<bool(const fdb5::Key&)>& matches,
                                               const Config& config) const;
};

//----------------------------------------------------------------------------------------------------------------------


}  // namespace fdb5

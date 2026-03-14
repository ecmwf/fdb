/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

/// @file   FamEngine.h
/// @author Metin Cakircali
/// @date   Mar 2026

#pragma once

#include "eckit/filesystem/URI.h"

#include "fdb5/database/Engine.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// FDB Engine implementation for the FAM backend.
///
/// The engine is a globally-registered singleton (via EngineBuilder).  It must
/// not make any FAM connections in its constructor — configuration is read
/// lazily the first time an operation requires it.
class FamEngine : public Engine {

public:  // methods

    /// Parse the root fam URI from config (fam_roots[0].uri).
    static eckit::URI rootURI(const Config& config);

    FamEngine() = default;

protected:  // methods

    std::string name() const override;
    std::string dbType() const override;

    eckit::URI location(const Key& key, const Config& config) const override;

    bool canHandle(const eckit::URI& uri, const Config& config) const override;

    std::vector<eckit::URI> visitableLocations(const Key& key, const Config& config) const override;
    std::vector<eckit::URI> visitableLocations(const metkit::mars::MarsRequest& rq,
                                               const Config& config) const override;

    void print(std::ostream& out) const override;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Emanuele Danovaro
/// @date   October 2023

#pragma once

#include "fdb5/database/Engine.h"

namespace fdb5::remote {

class RemoteEngineClient;

//----------------------------------------------------------------------------------------------------------------------

class RemoteEngine : public fdb5::Engine {

public: // methods

    static const char* typeName() { return "remote"; }

protected: // methods

    virtual std::string name() const override;

    virtual std::string dbType() const override;

    virtual bool canHandle(const eckit::URI& path) const override;

    virtual std::vector<eckit::URI> visitableLocations(const Config& config) const override;
    virtual std::vector<eckit::URI> visitableLocations(const metkit::mars::MarsRequest& rq, const Config& config) const override;

    virtual void print( std::ostream &out ) const override;

private:
    // mutable std::unique_ptr<RemoteEngineClient> client_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5::remote

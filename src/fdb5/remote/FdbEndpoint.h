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
/// @date   December 2023

#pragma once

#include "eckit/net/Endpoint.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

class FdbEndpoint : public eckit::net::Endpoint {

public: 

    FdbEndpoint(const std::string& endpoint, const std::string& domain = "") :
        eckit::net::Endpoint(endpoint), domain_(domain) {}
    FdbEndpoint(const std::string& host, int port, const std::string& domain = "") :
        eckit::net::Endpoint(host, port), domain_(domain) {}
    FdbEndpoint(const eckit::net::Endpoint& endpoint, const std::string& domain = "") :
        eckit::net::Endpoint(endpoint), domain_(domain) {}

    std::string hostname() const override { return host_+domain_; }

private:
    std::string domain_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5::remote

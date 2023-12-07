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
        eckit::net::Endpoint(endpoint), fullHostname_(host()+domain) {}
    FdbEndpoint(const std::string& host, int port, const std::string& domain = "") :
        eckit::net::Endpoint(host, port), fullHostname_(host+domain) {}
    FdbEndpoint(const eckit::net::Endpoint& endpoint, const std::string& domain = "") :
        eckit::net::Endpoint(endpoint), fullHostname_(endpoint.host()+domain) {}

    ~FdbEndpoint() override {}

    const std::string& hostname() const override { return fullHostname_; }


private:
    std::string fullHostname_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5::remote

template <>
struct std::hash<fdb5::remote::FdbEndpoint>
{
    std::size_t operator()(const fdb5::remote::FdbEndpoint& endpoint) const noexcept {
        const std::string& e = endpoint;
        return std::hash<std::string>{}(e);
    }
};
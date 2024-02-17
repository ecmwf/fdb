/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   S3Common.h
/// @author Nicolau Manubens
/// @date   Feb 2024

#pragma once

#include "eckit/filesystem/URI.h"

#include "fdb5/database/Key.h"
#include "fdb5/config/Config.h"

namespace fdb5 {

class S3Common {

public: // methods

    S3Common(const fdb5::Config&, const std::string& component, const fdb5::Key&);
    S3Common(const fdb5::Config&, const std::string& component, const eckit::URI&);

private: // methods

    void parseConfig(const fdb5::Config& config);

protected: // members

    eckit::net::Endpoint endpoint_;
    std::string db_bucket_;

    /// @note: code for single bucket for all DBs
    // std::string bucket_;
    // std::string db_prefix_;

private: // members

    std::string prefix_;

};

}
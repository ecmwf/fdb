/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   Mar 2018

#ifndef fdb5_config_FDBConfig_H
#define fdb5_config_FDBConfig_H


#include "eckit/config/LocalConfiguration.h"


namespace fdb5 {

class Schema;

//----------------------------------------------------------------------------------------------------------------------

class Config : public eckit::LocalConfiguration {

public: // methods

    Config();
    Config(const eckit::Configuration& config);

    /// Given a (potentially skeleton) configuration, expand it fully. This
    /// may involve loading a specific config.json
    Config expandConfig() const;

    virtual ~Config();

    /// Given paths of the form ~fdb, if FDB_HOME has been expanded in the configuration
    /// then do the expansion in here.
    eckit::PathName expandPath(const std::string& path) const;

    eckit::PathName schemaPath() const;
    eckit::PathName configPath() const;
    const Schema& schema() const;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_config_FDBConfig_H

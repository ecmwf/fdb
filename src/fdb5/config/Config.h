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
/// @author Tiago Quintino
/// @date   Mar 2018

#ifndef fdb5_config_Config_H
#define fdb5_config_Config_H

#include <sys/stat.h>  // for mode_t

#include <string>

#include "eckit/config/LocalConfiguration.h"
#include "eckit/filesystem/PathName.h"


namespace fdb5 {

class Schema;

//----------------------------------------------------------------------------------------------------------------------

class Config : public eckit::LocalConfiguration {
public:  // static methods

    static Config make(const eckit::PathName& path,
                       const eckit::Configuration& userConfig = eckit::LocalConfiguration());

public:  // methods

    Config();
    Config(const eckit::Configuration& config, const eckit::Configuration& userConfig = eckit::LocalConfiguration());

    /// Given a (potentially skeleton) configuration, expand it fully. This
    /// may involve loading a specific config.json
    Config expandConfig() const;

    ~Config() override;

    /// Given paths of the form ~fdb, if FDB_HOME has been expanded in the configuration
    /// then do the expansion in here.
    eckit::PathName expandPath(const std::string& path) const;


    void overrideSchema(const eckit::PathName& schemaPath, Schema* schema);
    const eckit::PathName& schemaPath() const;
    eckit::PathName configPath() const;

    const Schema& schema() const;

    mode_t umask() const;

    const eckit::Configuration& userConfig() const { return *userConfig_; }

    std::vector<Config> getSubConfigs(const std::string& name) const;
    std::vector<Config> getSubConfigs() const;

private:  // methods

    void initializeSchemaPath() const;

private:  // members

    mutable eckit::PathName schemaPath_;
    mutable bool schemaPathInitialised_;
    std::shared_ptr<eckit::LocalConfiguration> userConfig_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif  // fdb5_config_Config_H

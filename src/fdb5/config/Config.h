// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Simon Smart
/// @author Tiago Quintino
/// @date   Mar 2018

#pragma once

#include "eckit/config/LocalConfiguration.h"
#include "eckit/filesystem/PathName.h"

#include <sys/stat.h>  // for mode_t

#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace fdb5 {

class Schema;
class SelectMatcher;

//----------------------------------------------------------------------------------------------------------------------

class Config : public eckit::LocalConfiguration {
public:  // static methods

    static Config make(const eckit::PathName& path,
                       const eckit::Configuration& userConfig = eckit::LocalConfiguration(),
                       const std::string& fdb_home = "");

public:  // methods

    Config();
    Config(const eckit::Configuration& config, const eckit::Configuration& userConfig = eckit::LocalConfiguration());

    Config(const Config& other);
    Config& operator=(const Config& other);

    /// Given a (potentially skeleton) configuration, expand it fully. This
    /// may involve loading a specific config.json
    Config expandConfig() const;

    /// Given paths of the form ~fdb, if FDB_HOME has been expanded in the configuration
    /// then do the expansion in here.
    eckit::PathName expandPath(const std::string& path) const;


    void overrideSchema(const eckit::PathName& schemaPath, std::unique_ptr<Schema> schema);
    /// @note Return copy; a reference would race with overrideSchema().
    eckit::PathName schemaPath() const;
    eckit::PathName configPath() const;

    const Schema& schema() const;

    mode_t umask() const;

    const eckit::Configuration& userConfig() const { return *userConfig_; }

    size_t readIndexThreads() const;
    size_t apiQueueSize() const;
    size_t apiMaxQueues() const;

    std::vector<Config> getSubConfigs(const std::string& name) const;
    std::vector<Config> getSubConfigs() const;

    void setMatcher(std::unique_ptr<SelectMatcher> matcher);
    const SelectMatcher* matcher() const;

private:  // methods

    /// @pre schemaMutex_ must be held by the caller.
    void initializeSchemaPath() const;

private:  // members

    mutable eckit::PathName schemaPath_;
    mutable bool schemaPathInitialised_;
    mutable std::mutex schemaMutex_;
    std::shared_ptr<eckit::LocalConfiguration> userConfig_;
    std::shared_ptr<SelectMatcher> matcher_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/config/Config.h"

#include <algorithm>
#include <map>
#include <mutex>

#include "eckit/config/Resource.h"
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/filesystem/FileMode.h"
#include "eckit/runtime/Main.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/rules/Schema.h"

using namespace eckit;


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Config::Config() : schemaPath_("") {
    userConfig_ = std::make_shared<eckit::LocalConfiguration>(eckit::LocalConfiguration());
}

Config Config::make(const eckit::PathName& path, const eckit::Configuration& userConfig) {
    LOG_DEBUG_LIB(LibFdb5) << "Using FDB configuration file: " << path << std::endl;
    Config cfg{YAMLConfiguration(path)};
    cfg.set("configSource", path);
    cfg.userConfig_ = std::make_shared<eckit::LocalConfiguration>(userConfig);

    return cfg;
}

Config::Config(const Configuration& config, const eckit::Configuration& userConfig) :
    LocalConfiguration(config) {
    initializeSchemaPath();
    userConfig_ = std::make_shared<eckit::LocalConfiguration>(userConfig);
}

Config Config::expandConfig() const {
    // stops recursion on loading configuration of sub-fdb's
    if (has("type"))
        return *this;

    // If we have explicitly specified a config as an environment variable, use that

    char* config_str = ::getenv("FDB5_CONFIG");
    if (config_str) {
        std::string s(config_str);
        Config cfg{YAMLConfiguration(s)};
        cfg.set("configSource", "environment");
        if (!cfg.userConfig_) {
            cfg.userConfig_ = userConfig_;
        }
        return cfg;
    }

    // Consider possible config files

    PathName actual_path;
    bool found = false;

    // If the config file has been overridden, then use that directly.
    //
    // If fdb_home is explicitly set in the config then use that not from
    // the Resource (as it has been overridden, or this is a _nested_ config).

    std::string config_path = eckit::Resource<std::string>("fdb5ConfigFile;$FDB5_CONFIG_FILE", "");
    if (!config_path.empty() && !has("fdb_home")) {
        actual_path = config_path;
        if (!actual_path.exists())
            return *this;
        found = true;
    }

    // ~fdb is expanded from FDB_HOME (based on eckit::PathName::expandTilde()) or from the
    // 'fdb_home' value in config
    if (!found) {
        PathName configDir = expandPath("~fdb/etc/fdb");
        for (const std::string& stem :
             {Main::instance().displayName(), Main::instance().name(), std::string("config")}) {
            for (const char* tail : {".yaml", ".json"}) {
                actual_path = configDir / (stem + tail);
                if (actual_path.exists()) {
                    found = true;
                    break;
                }
            }
            if (found)
                break;
        }
    }

    if (found) {
        return Config::make(actual_path, userConfig_ ? *userConfig_ : eckit::LocalConfiguration());
    }

    // No expandable config available. Use the skeleton config.
    return *this;
}


Config::~Config() {}


// TODO: We could add this to expandTilde.

PathName Config::expandPath(const std::string& path) const {
    // If path starts with ~, split off the first component. If that is supplied in
    // the configuration, then use that instead!

    ASSERT(path.size() > 0);
    if (path[0] == '~') {
        if (path.length() > 1 && path[1] != '/') {
            size_t slashpos = path.find('/');
            if (slashpos == std::string::npos)
                slashpos = path.length();

            std::string key = path.substr(1, slashpos - 1) + "_home";
            std::transform(key.begin(), key.end(), key.begin(), ::tolower);

            if (has(key)) {
                std::string newpath = getString(key) + path.substr(slashpos);
                return PathName(newpath);
            }
        }
    }

    /// use the default expansion if unspecified (eckit::Main will expand FDB_HOME)
    return PathName(path);
}

const PathName& Config::schemaPath() const {
    if (schemaPath_.path().empty()) {
        initializeSchemaPath();
    }
    return schemaPath_;
}

void Config::initializeSchemaPath() const {

    // If the user has specified the schema location in the FDB config, use that,
    // otherwise use the library-wide schema path.

    if (has("schema")) {
        schemaPath_ = expandPath(getString("schema"));
    }
    else {
        // TODO: deduplicate this with the library-level schemaPath()
        //       N.B. this uses Config expandPath()
        static std::string fdbSchemaFile =
            Resource<std::string>("fdbSchemaFile;$FDB_SCHEMA_FILE", "~fdb/etc/fdb/schema");

        schemaPath_ = expandPath(fdbSchemaFile);
    }

    LOG_DEBUG_LIB(LibFdb5) << "Using FDB schema: " << schemaPath_ << std::endl;
}

PathName Config::configPath() const {
    return getString("configSource", "unknown");
}

const Schema& Config::schema() const {
    return SchemaRegistry::instance().get(schemaPath());
}

mode_t Config::umask() const {
    if (has("permissions")) {
        return FileMode(getString("permissions")).mask();
    }
    static eckit::FileMode fdbFileMode(
        eckit::Resource<std::string>("fdbFileMode", std::string("0644")));
    return fdbFileMode.mask();
}

std::vector<Config> Config::getSubConfigs(const std::string& name) const {
    std::vector<Config> out;

    for (auto configuration : getSubConfigurations(name)) {
        Config config{configuration};
        config.userConfig_ = userConfig_;
        out.push_back(config);
    }
    return out;
}

std::vector<Config> Config::getSubConfigs() const {
    std::vector<Config> out;

    for (auto configuration : getSubConfigurations()) {
        Config config{configuration};
        config.userConfig_ = userConfig_;
        out.push_back(config);
    }
    return out;
}


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

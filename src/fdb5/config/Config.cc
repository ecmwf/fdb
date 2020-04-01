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

#include <map>
#include <mutex>
#include <algorithm>

#include "eckit/config/Resource.h"
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/runtime/Main.h"
#include "eckit/filesystem/FileMode.h"

#include "fdb5/rules/Schema.h"
#include "fdb5/LibFdb5.h"

using namespace eckit;


namespace fdb5 {


/// Schemas are persisted in this registry
///
class SchemaRegistry {
public:

    static SchemaRegistry& instance() {
        static SchemaRegistry me;
        return me;
    }

    const Schema& get(const PathName& path) {
        std::lock_guard<std::mutex> lock(m_);
        auto it = schemas_.find(path);
        if (it != schemas_.end()) {
            return *it->second;
        }

        schemas_[path] = std::unique_ptr<Schema>(new Schema(path));
        return *schemas_[path];
    }
private:
    std::mutex m_;
    std::map<PathName, std::unique_ptr<Schema>> schemas_;
};

//----------------------------------------------------------------------------------------------------------------------

Config::Config() {}


Config::Config(const Configuration& config) :
    LocalConfiguration(config) {}

Config Config::expandConfig() const {

    // stops recursion on loading configuration of sub-fdb's
    if (has("type")) return *this;

    // If we have explicitly specified a config as an environment variable, use that

    char* config_str = ::getenv("FDB5_CONFIG");
    if (config_str) {
        std::string s(config_str);
        Config cfg{YAMLConfiguration(s)};
        cfg.set("configSource", "environment");
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
        if (!actual_path.exists()) return *this;
        found = true;
    }

    // ~fdb is expanded from FDB_HOME (based on eckit::PathName::expandTilde()) or from the 'fdb_home' value in config
    if (!found) {
        PathName configDir = expandPath("~fdb/etc/fdb");
        for (const std::string& stem : {Main::instance().displayName(),
                                        Main::instance().name(),
                                        std::string("config")}) {

            for (const char* tail : {".yaml", ".json"}) {
                actual_path = configDir / (stem + tail);
                if (actual_path.exists()) {
                    found = true;
                    break;
                }
            }
            if (found) break;
        }
    }

    if (found) {
        eckit::Log::debug<LibFdb5>() << "Using FDB configuration file: " << actual_path << std::endl;
        Config cfg{YAMLConfiguration(actual_path)};
        cfg.set("configSource", actual_path);
        return cfg;
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

            std::string key = path.substr(1, slashpos-1) + "_home";
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

PathName Config::schemaPath() const {

    // If the user has specified the schema location in the FDB config, use that,
    // otherwise use the library-wide schema path.

    if (has("schema")) {
        return expandPath(getString("schema"));
    }

    // TODO: deduplicate this with the library-level schemaPath() [n.b. this uses Config expandPath()]
    static std::string fdbSchemaFile = Resource<std::string>("fdbSchemaFile;$FDB_SCHEMA_FILE", "~fdb/etc/fdb/schema");

    return expandPath(fdbSchemaFile);
}

PathName Config::configPath() const {
    return getString("configSource", "unknown");
}

const Schema& Config::schema() const {
    PathName path(schemaPath());
    return SchemaRegistry::instance().get(path);
}

mode_t Config::umask() const {
    if(has("permissions")) {
        return FileMode(getString("permissions")).mask();
    }
    static eckit::FileMode fdbFileMode(eckit::Resource<std::string>("fdbFileMode", std::string("0644")));
    return fdbFileMode.mask();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

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

#include "fdb5/rules/Schema.h"
#include "fdb5/LibFdb.h"

#include "eckit/config/Resource.h"
#include "eckit/config/YAMLConfiguration.h"

#include <map>
#include <mutex>

using namespace eckit;


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Config::Config() {}


Config::Config(const Configuration& config) :
    LocalConfiguration(config) {}

Config Config::expandConfig() const {

    // If the config is already initialised, then use it directly.
    if (has("type")) return *this;

    // If we have explicitly specified a config as an environment variable, use that

    char* config_str = ::getenv("FDB5_CONFIG");
    if (config_str) {
        std::string s(config_str);
        eckit::YAMLConfiguration cfg(s);
        return cfg;
    }

    // Otherwise, if we have specified a configuration path (including in the config
    // being expanded) then read that and use it.

    const std::string default_config_path = "~fdb/etc/fdb/config.yaml";
    std::string config_path = eckit::Resource<std::string>("fdb5ConfigFile;$FDB5_CONFIG_FILE", default_config_path);

    // If fdb_home is explicitly set in the config then use that not from
    // the Resource (as it has been overridden, or this is a _nested_ config).

    if (has("fdb_home")) config_path = default_config_path;

    eckit::PathName actual_path = expandPath(config_path);
    if (actual_path.exists()) {
        eckit::Log::debug<LibFdb>() << "Using FDB configuration file: " << actual_path << std::endl;
        eckit::YAMLConfiguration cfg(actual_path);
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

const Schema& Config::schema() const {

    static std::mutex m;
    static std::map<PathName, std::unique_ptr<Schema>> schemaMap;

    std::lock_guard<std::mutex> lock(m);

    PathName path(schemaPath());

    auto it = schemaMap.find(path);
    if (it != schemaMap.end()) return *it->second;

    schemaMap[path] = std::unique_ptr<Schema>(new Schema(path));
    return *schemaMap[path];
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

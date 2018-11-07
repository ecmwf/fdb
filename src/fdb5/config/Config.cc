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

#include "eckit/config/Resource.h"

#include <map>
#include <mutex>

using namespace eckit;


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Config::Config() {}


Config::Config(const Configuration& config) :
    LocalConfiguration(config) {}


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

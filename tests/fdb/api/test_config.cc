/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cstdlib>

#include "eckit/testing/Test.h"

#include "fdb5/config/Config.h"

using namespace eckit::testing;
using namespace eckit;


namespace fdb {
namespace test {

//----------------------------------------------------------------------------------------------------------------------

class SetenvCleanup {

public:
    SetenvCleanup(const std::string& key, const std::string& value):
        key_(key) {

        char* old = ::getenv(key.c_str());
        if (old) oldValue_ = std::string(old);

        ::setenv(key.c_str(), value.c_str(), true);
    }

    ~SetenvCleanup() {
        if (oldValue_.empty()) {
            ::unsetenv(key_.c_str());
        } else {
            ::setenv(key_.c_str(), oldValue_.c_str(), true);
        }
    }

private:
    std::string key_;
    std::string oldValue_;
};

//----------------------------------------------------------------------------------------------------------------------

CASE( "config_expands_from_environment_variable_json" ) {

    const std::string config_str(R"XX(
        {
            "type": "local",
            "engine": "pmem",
            "groups": [{
                "pools": [{
                    "path": "/a/path/is/something"
                }]
            }]
        }
    )XX");

    SetenvCleanup env("FDB5_CONFIG", config_str);
    Log::info() << "Config_str" << (void*)::getenv("FDB5_CONFIG") << std::endl;

    fdb5::Config expanded = fdb5::Config().expandConfig();

    Log::info() << "Expanded: " << expanded << std::endl;

    EXPECT(expanded.getString("type") == "local");
    EXPECT(expanded.getString("engine") == "pmem");
    EXPECT(expanded.getSubConfigurations("groups").size() == 1);
    EXPECT(expanded.getSubConfigurations("groups")[0].getSubConfigurations("pools").size() == 1);
    EXPECT(expanded.getSubConfigurations("groups")[0].getSubConfigurations("pools")[0].getString("path") == "/a/path/is/something");
}


CASE( "config_expands_from_environment_variable_yaml" ) {

    const std::string config_str(R"XX(
        ---
        type: local
        engine: toc
        spaces:
        - roots:
          - path: "/a/path/is/something"
    )XX");

    SetenvCleanup env("FDB5_CONFIG", config_str);
    Log::info() << "Config_str" << (void*)::getenv("FDB5_CONFIG") << std::endl;

    fdb5::Config expanded = fdb5::Config().expandConfig();

    Log::info() << "Expanded: " << expanded << std::endl;

    EXPECT(expanded.getString("type") == "local");
    EXPECT(expanded.getString("engine") == "toc");
    EXPECT(expanded.getSubConfigurations("spaces").size() == 1);
    EXPECT(expanded.getSubConfigurations("spaces")[0].getSubConfigurations("roots").size() == 1);
    EXPECT(expanded.getSubConfigurations("spaces")[0].getSubConfigurations("roots")[0].getString("path") == "/a/path/is/something");
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include <cstdlib>

#include "eckit/config/Resource.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpFile.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/io/DataHandle.h"
#include "eckit/testing/Test.h"

#include "fdb5/config/Config.h"
#include "fdb5/legacy/LegacyArchiver.h"

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

    fdb5::Config expanded = fdb5::Config().expandConfig();

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

    fdb5::Config expanded = fdb5::Config().expandConfig();

    EXPECT(expanded.getString("type") == "local");
    EXPECT(expanded.getString("engine") == "toc");
    EXPECT(expanded.getSubConfigurations("spaces").size() == 1);
    EXPECT(expanded.getSubConfigurations("spaces")[0].getSubConfigurations("roots").size() == 1);
    EXPECT(expanded.getSubConfigurations("spaces")[0].getSubConfigurations("roots")[0].getString("path") == "/a/path/is/something");
}

CASE( "config_expands_explicit_path" ) {

    const std::string config_str(R"XX(
        ---
        type: local
        engine: toc
        spaces:
        - roots:
          - path: "/a/path/is/different"
    )XX");

    eckit::TmpFile tf;

    {
        std::unique_ptr<eckit::DataHandle> dh(tf.fileHandle());
        eckit::AutoClose close(*dh);
        eckit::Length estimate;
        dh->openForWrite(estimate);
        dh->write(config_str.c_str(), config_str.size());
    }

    SetenvCleanup env("FDB5_CONFIG_FILE", tf.asString());

    fdb5::Config expanded = fdb5::Config().expandConfig();

    EXPECT(expanded.getString("type") == "local");
    EXPECT(expanded.getString("engine") == "toc");
    EXPECT(expanded.getSubConfigurations("spaces").size() == 1);
    EXPECT(expanded.getSubConfigurations("spaces")[0].getSubConfigurations("roots").size() == 1);
    EXPECT(expanded.getSubConfigurations("spaces")[0].getSubConfigurations("roots")[0].getString("path") == "/a/path/is/different");
}

CASE( "config_expands_override_fdb_home" ) {

    const std::string config_str(R"XX(
        ---
        type: local
        engine: toc
        spaces:
        - roots:
          - path: /a/path/is/first
          - path: /a/path/is/second
    )XX");

    eckit::TmpDir td;

    (td / "etc/fdb").mkdir();

    {
        std::unique_ptr<eckit::DataHandle> dh((td / "etc/fdb/config.yaml").fileHandle());
        eckit::AutoClose close(*dh);
        eckit::Length estimate;
        dh->openForWrite(estimate);
        dh->write(config_str.c_str(), config_str.size());
    }

    fdb5::Config cfg;
    cfg.set("fdb_home", td.asString());

    fdb5::Config expanded = cfg.expandConfig();

    EXPECT(expanded.getString("type") == "local");
    EXPECT(expanded.getString("engine") == "toc");
    EXPECT(expanded.getSubConfigurations("spaces").size() == 1);
    EXPECT(expanded.getSubConfigurations("spaces")[0].getSubConfigurations("roots").size() == 2);
    EXPECT(expanded.getSubConfigurations("spaces")[0].getSubConfigurations("roots")[0].getString("path") == "/a/path/is/first");
    EXPECT(expanded.getSubConfigurations("spaces")[0].getSubConfigurations("roots")[1].getString("path") == "/a/path/is/second");
}


CASE( "legacy_archiver_subtoc" ) {

    eckit::TmpDir td;

    (td / "etc/fdb").mkdir();
    (td / "root").mkdir();

    const std::string config_str(R"XX(
        ---
        type: local
        engine: toc
        spaces:
        - roots:
          - path: )XX" + (td / "root").path());

    {
        std::unique_ptr<eckit::DataHandle> dh((td / "etc/fdb/config.yaml").fileHandle());
        eckit::AutoClose close(*dh);
        eckit::Length estimate;
        dh->openForWrite(estimate);
        dh->write(config_str.c_str(), config_str.size());
    }

    eckit::LocalConfiguration applicationConfig;
    applicationConfig.set("fdb_home", td.asString());
    applicationConfig.set("type", "fdb5");
    applicationConfig.set("useSubToc", false);

    {    
        fdb5::legacy::LegacyArchiver archiver(applicationConfig);
        const fdb5::Config& config = archiver.fdb().config();

        EXPECT(config.getString("type") == "local");
        EXPECT(config.getString("engine") == "toc");
        EXPECT(config.getSubConfigurations("spaces").size() == 1);
        EXPECT(config.getSubConfigurations("spaces")[0].getSubConfigurations("roots").size() == 1);
        EXPECT(config.getSubConfigurations("spaces")[0].getSubConfigurations("roots")[0].getString("path") == (td / "root").path());
        EXPECT(config.userConfig().has("useSubToc"));
        EXPECT(config.userConfig().getBool("useSubToc") == false);
    }
    applicationConfig.set("useSubToc", true);
    {    
        fdb5::legacy::LegacyArchiver archiver(applicationConfig);
        const fdb5::Config& config = archiver.fdb().config();

        EXPECT(config.getString("type") == "local");
        EXPECT(config.getString("engine") == "toc");
        EXPECT(config.getSubConfigurations("spaces").size() == 1);
        EXPECT(config.getSubConfigurations("spaces")[0].getSubConfigurations("roots").size() == 1);
        EXPECT(config.getSubConfigurations("spaces")[0].getSubConfigurations("roots")[0].getString("path") == (td / "root").path());
        EXPECT(config.userConfig().has("useSubToc"));
        EXPECT(config.userConfig().getBool("useSubToc") == true);
    }

}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}

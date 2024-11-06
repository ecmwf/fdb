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
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

/// @file   test_fam_common.h
/// @author Metin Cakircali
/// @date   Jun 2024

#pragma once

#include "eckit/config/YAMLConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/DataHandle.h"
#include "eckit/log/Log.h"
#include "fdb5/LibFdb5.h"

#include <string.h>

#include <fstream>
#include <regex>

using namespace std::string_literals;

namespace {
// const auto TEST_FDB_ENDPOINT = eckit::net::Endpoint("127.0.0.1", 8880);
const auto TEST_FDB_FAM_ENDPOINT = "10.115.3.2:8080"s;
const auto TEST_FDB_FAM_REGION   = "test_region_fdb"s;
const auto TEST_FDB_FAM_URI      = "fam://" + TEST_FDB_FAM_ENDPOINT + "/" + TEST_FDB_FAM_REGION;
}  // namespace

//----------------------------------------------------------------------------------------------------------------------

#define TEST_LOG_INFO(msg)  eckit::Log::info() << "INFO  [TEST_FAM] : " << msg << std::endl
#define TEST_LOG_DEBUG(msg) eckit::Log::debug<fdb5::LibFdb5>() << "DEBUG [TEST_FAM] : " << msg << std::endl

//----------------------------------------------------------------------------------------------------------------------

namespace fdb::test::fam {

void readAndValidate(eckit::DataHandle* dh, const char* data, const long length) {
    TEST_LOG_INFO("READ");

    dh->openForRead();

    std::string tmp;
    tmp.resize(length);
    char* buffer = &tmp[0];

    const auto rlen = dh->read(buffer, length);

    TEST_LOG_INFO("VALIDATE");

    ASSERT(rlen == length);

    ASSERT(::memcmp(data, buffer, length) == 0);

    dh->close();
}

void write(const std::string& buffer, const eckit::PathName& path) {
    std::ofstream file(path);
    if (!file) { throw eckit::CantOpenFile(path); }
    file << buffer;
}

struct FamSetup {
    FamSetup(const std::string& schema, const std::string& config) {
        // TEST_LOG_INFO("- Working directory: " << cwd_);
        // TEST_LOG_INFO("- Config path: " << configPath);
        // TEST_LOG_INFO("- Schema path: " << schemaPath);
        cwd_.mkdir();
        write(schema, schemaPath);
        write(std::regex_replace(config, std::regex("./schema"), schemaPath.asString()), configPath.asString());
    }

    const eckit::LocalPathName cwd_ {eckit::LocalPathName::cwd() + "/" + "fam_test_dir"};

    const eckit::PathName schemaPath {cwd_ + "/" + "schema"};
    const eckit::PathName configPath {cwd_ + "/" + "config.yaml"};
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test::fam

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

#include <cstring>
#include <fstream>
#include <iostream>
#include <ostream>
#include <regex>
#include <string>

#include "eckit/config/YAMLConfiguration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/io/DataHandle.h"
#include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"

using namespace std::string_literals;

namespace {
const auto test_fdb_fam_endpoint = "172.30.0.2:8880"s;
const auto test_fdb_fam_region   = "test_region_fdb"s;
const auto test_fdb_fam_uri      = "fam://" + test_fdb_fam_endpoint + "/" + test_fdb_fam_region;
}  // namespace

//----------------------------------------------------------------------------------------------------------------------

#define TEST_LOG_INFO(msg) eckit::Log::info() << "INFO  [TEST_FAM] : " << msg << std::endl
#define TEST_LOG_DEBUG(msg) eckit::Log::debug<fdb5::LibFdb5>() << "DEBUG [TEST_FAM] : " << msg << std::endl

//----------------------------------------------------------------------------------------------------------------------

namespace fdb::test::fam {

inline void read_and_validate(eckit::DataHandle* dh, const char* data, const long length) {
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

inline void write(const std::string& buffer, const eckit::PathName& path) {
    std::ofstream file(path);
    if (!file) {
        throw eckit::CantOpenFile(path);
    }
    file << buffer;
}

inline int count_list(fdb5::ListIterator& list) {
    int count = 0;
    fdb5::ListElement elem;
    while (list.next(elem)) {
        elem.print(std::cout, true, true, false, " ");
        std::cout << '\n';
        ++count;
    }
    return count;
}

struct FamSetup {
    FamSetup(const std::string& schema, std::string config) {
        // cwd_.mkdir();
        eckit::LocalPathName root_dir(cwd_ + "/" + "root");
        root_dir.mkdir();
        write(schema, schemaPath);
        config = std::regex_replace(config, std::regex("./schema"), schemaPath.asString());
        config = std::regex_replace(config, std::regex("./root"), root_dir.c_str());
        write(config, configPath.asString());
    }

    // const eckit::LocalPathName cwd_{eckit::LocalPathName::cwd() + "/" + "fam_test_dir"};
    eckit::TmpDir cwd_{eckit::LocalPathName::cwd().c_str()};

    eckit::PathName schemaPath{cwd_ + "/" + "schema"};
    eckit::PathName configPath{cwd_ + "/" + "config.yaml"};
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test::fam

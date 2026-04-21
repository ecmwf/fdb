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

#include <signal.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
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
#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/fam/FamPath.h"
#include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/ListIterator.h"

using namespace std::string_literals;

//----------------------------------------------------------------------------------------------------------------------

#define TEST_LOG_INFO(msg) eckit::Log::info() << "INFO  [TEST_FAM] : " << msg << std::endl
#define TEST_LOG_DEBUG(msg) eckit::Log::debug<fdb5::LibFdb5>() << "DEBUG [TEST_FAM] : " << msg << std::endl

//----------------------------------------------------------------------------------------------------------------------

namespace fdb::test::fam {

/// Derives the POSIX shm name from an endpoint (host part only, matching the mock's cisServer key).
inline std::string shmNameFromEndpoint(const std::string& endpoint) {
    auto colon = endpoint.rfind(':');
    auto host = (colon != std::string::npos) ? endpoint.substr(0, colon) : endpoint;
    std::transform(host.begin(), host.end(), host.begin(),
                   [](unsigned char ch) { return std::isalnum(ch) ? static_cast<char>(ch) : '_'; });
    return "/eckit_fam_mock_" + (host.empty() ? "default" : host);
}

/// Per-process unique endpoint with atexit cleanup.
inline const std::string test_fdb_fam_endpoint = []() -> std::string {
    const char* ep = std::getenv("ECKIT_FAM_TEST_ENDPOINT");
    auto base = ep ? std::string(ep) : std::string("localhost:8880");
    auto colon = base.rfind(':');
    std::string endpoint;
    if (colon == std::string::npos) {
        endpoint = base + "_" + std::to_string(::getpid()) + ":0";
    }
    else {
        auto host = base.substr(0, colon);
        auto port = base.substr(colon);
        endpoint = host + "_" + std::to_string(::getpid()) + port;
    }
    static std::string shm_name = shmNameFromEndpoint(endpoint);
    std::atexit([] { ::shm_unlink(shm_name.c_str()); });
    return endpoint;
}();

// const auto test_fdb_fam_region = eckit::FamPath("test_region_fdb");
// const auto test_fdb_fam_uri = "fam://" + test_fdb_fam_endpoint + "/" + test_fdb_fam_region.asString();

inline void read_and_validate(eckit::DataHandle* dh, const char* data, const long length) {
    TEST_LOG_INFO("READ");

    dh->openForRead();

    std::string tmp;
    tmp.resize(length);
    char* buffer = tmp.data();

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

inline int count_list(fdb5::ListIterator& list, std::ostream& out = std::cout) {
    int count = 0;
    fdb5::ListElement elem;
    while (list.next(elem)) {
        elem.print(out, true, true, false, " ");
        out << '\n';
        ++count;
    }
    return count;
}

/// Default 3-level test schema shared across store and catalogue tests.
const std::string test_schema =
    "[ fam1a, fam1b, fam1c\n"
    "    [ fam2a, fam2b, fam2c\n"
    "       [ fam3a, fam3b, fam3c ]]]\n";

/// Build a YAML config string for a FAM-backed FDB test.
/// @param fam_uri    e.g. "fam://host:port/region"
/// @param catalogue  "toc" or "fam"
/// @param engine     empty for toc-based, "fam" for fam catalogue
inline std::string makeTestConfig(const std::string& fam_uri, const std::string& catalogue = "fam",
                                  const std::string& engine = "fam") {
    std::string config =
        "---\n"
        "type: local\n";
    if (!engine.empty()) {
        config += "engine: " + engine + "\n";
    }
    config +=
        "schema: ./schema\n"
        "store: fam\n"
        "catalogue: " +
        catalogue +
        "\n"
        "spaces:\n"
        "- handler: Default\n"
        "  roots:\n"
        "  - path: ./root\n"
        "fam_roots:\n"
        "- uri: " +
        fam_uri +
        "\n"
        "  writable: true\n"
        "  visit: true\n";
    return config;
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

//----------------------------------------------------------------------------------------------------------------------

namespace fdb::test {

/// Fork @p n child processes. Each child waits on a barrier, then runs @p fn(child_index).
/// Parent waits for all children to exit.
/// Returns true if every child exited with status 0.
template <typename Fn>
bool forkAndRun(int n, Fn&& fn) {
    int barrier[2];
    if (::pipe(barrier) != 0) {
        return false;
    }

    std::vector<pid_t> pids;
    pids.reserve(n);

    for (int i = 0; i < n; ++i) {
        const pid_t pid = ::fork();
        if (pid < 0) {
            for (auto p : pids) {
                ::kill(p, SIGTERM);
                ::waitpid(p, nullptr, 0);
            }
            ::close(barrier[0]);
            ::close(barrier[1]);
            return false;
        }
        if (pid == 0) {
            std::set_terminate([]() { ::_exit(1); });
            ::close(barrier[1]);
            char buf;
            static_cast<void>(::read(barrier[0], &buf, 1));
            ::close(barrier[0]);
            try {
                fn(i);
                ::_exit(0);
            }
            catch (...) {
                ::_exit(1);
            }
        }
        pids.push_back(pid);
    }

    ::close(barrier[0]);
    ::close(barrier[1]);

    bool all_ok = true;
    for (auto pid : pids) {
        int status = 0;
        if (::waitpid(pid, &status, 0) < 0) {
            all_ok = false;
        }
        else if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            all_ok = false;
        }
    }
    return all_ok;
}

/// Fork a single child that runs @p fn. Parent blocks until child completes.
/// Returns true if child exited with status 0.
template <typename Fn>
bool forkWriter(Fn&& fn) {
    int barrier[2];
    if (::pipe(barrier) != 0) {
        return false;
    }

    const pid_t pid = ::fork();
    if (pid < 0) {
        ::close(barrier[0]);
        ::close(barrier[1]);
        return false;
    }
    if (pid == 0) {
        std::set_terminate([]() { ::_exit(1); });
        ::close(barrier[0]);
        try {
            fn();
            ::close(barrier[1]);
            ::_exit(0);
        }
        catch (...) {
            ::close(barrier[1]);
            ::_exit(1);
        }
    }

    ::close(barrier[1]);
    char buf;
    static_cast<void>(::read(barrier[0], &buf, 1));
    ::close(barrier[0]);

    int status = 0;
    ::waitpid(pid, &status, 0);
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

}  // namespace fdb::test

/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include "eckit/config/ResourceMgr.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"

#include <fcntl.h>
#include <unistd.h>
#include <csignal>
#include <filesystem>

using eckit::Log;
using eckit::ResourceMgr;
using fdb5::Config;
using fdb5::FDB;
using fdb5::FDBToolRequest;


namespace {

std::filesystem::path get_fdb_server_path() {
    const char* path = std::getenv("FDB_SERVER_EXECUTABLE");
    if (path == nullptr) {
        Log::error() << "FDB_SERVER_EXECUTABLE not set, cannot find fdb-server executable" << std::endl;
        throw std::runtime_error("FDB_SERVER_EXECUTABLE not set. Test cannot find fdb-server executable.");
    }
    return path;
}

pid_t run_server(const std::filesystem::path& fdb_server_path, const std::filesystem::path& log_file) {
    pid_t pid = fork();
    if (pid == -1) {
        std::stringstream ss;
        ss << "Fork failed: " << std::strerror(errno);
        throw std::runtime_error(ss.str());
    }

    if (pid > 0) {
        sleep(1);
        Log::info() << "Started fdb-server pid: " << pid << std::endl;
        return pid;
    }

    if (auto rc = setpgid(0, 0); rc != 0) {
        Log::error() << "Setting process group id for fdb-server failed: " << std::strerror(errno) << std::endl;
        std::exit(-1);
    }

    int fd = open(log_file.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    dup2(fd, 1);
    dup2(fd, 2);
    close(fd);

    std::array<const char*, 2> argv       = {"fdb-server", nullptr};
    const auto server_config_path         = std::filesystem::current_path() / "fdb_server_config.yaml";
    const std::string fdb_config_file_env = std::string("FDB5_CONFIG_FILE=") + server_config_path.string();
    std::array<const char*, 3> env        = {fdb_config_file_env.c_str(), nullptr};
    if (int rc = execve(fdb_server_path.c_str(), const_cast<char**>(argv.data()), const_cast<char**>(env.data()));
        rc == -1) {
        Log::error() << "Exec fdb-server failed: " << std::strerror(errno) << std::endl;
        std::exit(-1);
    }
    return {};
}

void kill_server(const pid_t pid) {
    int rc = kill(pid, SIGKILL);
    Log::info() << "Killing " << pid << std::endl;
    if (rc == -1) {
        Log::error() << "Could not kill fdb-server: " << std::strerror(errno) << std::endl;
    }
    sleep(1);
    EXPECT_EQUAL(rc, 0);
}
// Regression test for issue FDB-419.
// The underlying issue was that broken connections were not replaced with new connections
// so that once a conection to a fdb-server was broken no new connection was establish to
// this server.
// NOTE: This test changes the PPID of the forked fdb-server so that the whole process tree
// can be killed on MacOS like on Linux. This however means that if ctest is interrupted with
// CTRL-C, SIGINT will only be sent to the test and no longer to the fdb-server process, 
// resulting in orphaned processes.
CASE("FDB-419") {
    const auto fdb_server_path = get_fdb_server_path();
    auto fdb_server_pid        = run_server(fdb_server_path, "srv1.log");

    const auto config = Config::make("fdb_client_config.yaml");
    auto fdb_a        = FDB(config);
    auto fdb_b        = FDB(config);

    const auto req = fdb5::FDBToolRequest::requestsFromString("class=rd,expver=xxxx")[0];
    EXPECT_NO_THROW(fdb_a.list(req));
    EXPECT_NO_THROW(fdb_b.list(req));
    kill_server(-fdb_server_pid);
    EXPECT_THROWS(fdb_a.list(req));

    fdb_server_pid = run_server(fdb_server_path, "srv2.log");
    EXPECT_NO_THROW(fdb_a.list(req));
    EXPECT_NO_THROW(fdb_b.list(req));
    kill_server(-fdb_server_pid);
}
}  // namespace
int main(int argc, char** argv) {
    return eckit::testing::run_tests(argc, argv);
}

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
#include "eckit/filesystem/PathName.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"

#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <csignal>
#include <fstream>
#include <sstream>

using eckit::Log;
using eckit::PathName;
using eckit::ResourceMgr;
using fdb5::Config;
using fdb5::FDB;
using fdb5::FDBToolRequest;

using std::literals::chrono_literals::operator""ms;
using std::literals::chrono_literals::operator""s;
extern char** environ;


namespace {

void spawn_reaper(const pid_t child_pid) {
    pid_t pid = fork();
    if (pid == -1) {
        std::ostringstream ss;
        ss << "Failed to fork reaper: " << std::strerror(errno);
        throw std::runtime_error(ss.str());
    }
    if (pid > 0) {
        Log::info() << "Started reaper process" << pid << std::endl;
        return;
    }
    setsid();
    for (;;) {
        std::this_thread::sleep_for(200ms);
        if (const auto p = getppid(); p == 1) {
            kill(child_pid, SIGKILL);
            std::exit(0);
        }
    }
}

PathName get_fdb_server_path() {
    const char* path = std::getenv("FDB_SERVER_EXECUTABLE");
    if (path == nullptr) {
        Log::error() << "FDB_SERVER_EXECUTABLE not set, cannot find fdb-server executable" << std::endl;
        throw std::runtime_error("FDB_SERVER_EXECUTABLE not set. Test cannot find fdb-server executable.");
    }
    return path;
}

PathName get_cwd() {
    std::vector<char> buf(4096);
    const auto cwd = getcwd(buf.data(), buf.size());
    if (cwd == nullptr) {
        std::ostringstream ss;
        ss << "Failed to get current working directory " << std::strerror(errno);
        throw std::runtime_error(ss.str());
    }
    return cwd;
}

std::vector<const char*> copy_environment() {
    std::vector<const char*> env{};
    for (char** p = environ; *p != nullptr; ++p) {
        env.emplace_back(*p);
    }
    return env;
}

pid_t run_server(const PathName& fdb_server_path, const PathName& log_file) {
    pid_t pid = fork();
    if (pid == -1) {
        std::ostringstream ss;
        ss << "Fork failed: " << std::strerror(errno);
        throw std::runtime_error(ss.str());
    }

    if (pid > 0) {
        // The spawned child changes its pgid and hence does not recieve SIGINT if ctest is killed with CTRL-C. We need
        // to create a watchdog that kills the child when the parent dies. On linux that can be done with
        // 'prctl(PR_SET_DEATHSIG, SIGKILL)' but there is no such functionality available for MacOS.
        spawn_reaper(pid);
        std::this_thread::sleep_for(1s);
        Log::info() << "Started fdb-server pid: " << pid << std::endl;
        return pid;
    }

    if (auto rc = setpgid(0, 0); rc != 0) {
        Log::error() << "Setting process group id for fdb-server failed: " << std::strerror(errno) << std::endl;
        std::exit(-1);
    }

    int fd = open(log_file.asString().c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    dup2(fd, 1);
    dup2(fd, 2);
    close(fd);

    std::array<const char*, 2> argv       = {"fdb-server", nullptr};
    const auto server_config_path         = get_cwd() / "fdb_server_config.yaml";
    auto env                              = copy_environment();
    const std::string fdb_config_file_env = std::string("FDB5_CONFIG_FILE=") + server_config_path.asString();
    env.emplace_back(fdb_config_file_env.c_str());
    env.emplace_back(nullptr);

    if (int rc =
            execve(fdb_server_path.asString().c_str(), const_cast<char**>(argv.data()), const_cast<char**>(env.data()));
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

    std::this_thread::sleep_for(1s);
    EXPECT_EQUAL(rc, 0);
}

size_t count_in_file(const PathName& file, const std::string& text) {
    std::ostringstream oss;
    std::ifstream in(file.asString());
    oss << in.rdbuf();
    std::string content = oss.str();
    size_t count        = 0;
    size_t pos          = 0;
    while ((pos = content.find(text, pos)) != std::string::npos) {
        ++count;
        pos += text.length();
    }
    return count;
}

// Regression test for issue FDB-419.
// The underlying issue was that broken connections were not replaced with new connections
// so that once a conection to a fdb-server was broken no new connection was establish to
// this server.
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
    // Ensure only one connection is established with each server
    EXPECT_EQUAL(count_in_file("srv1.log", "FDB forked pid"), 1);
    EXPECT_EQUAL(count_in_file("srv2.log", "FDB forked pid"), 1);
}
}  // namespace
int main(int argc, char** argv) {
    return eckit::testing::run_tests(argc, argv);
}

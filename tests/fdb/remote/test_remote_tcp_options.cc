// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// The TCP keepalive and user timeout settings must reach the sockets of remote connections. The settings are passed
// as environment variables, see CMakeLists.txt.

#include "fdb5/remote/Connection.h"

#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPServer.h"
#include "eckit/testing/Test.h"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include <future>

using namespace eckit::testing;

namespace fdb5::test {

namespace {

int getSocketOption(int fd, int level, int option) {
    int value       = -1;
    socklen_t size  = sizeof(value);
    const int error = ::getsockopt(fd, level, option, &value, &size);
    EXPECT_EQUAL(error, 0);
    return value;
}

}  // namespace

CASE("The configured keepalive and user timeout are set on a connected socket") {

    eckit::net::EphemeralTCPServer server;
    auto accepted = std::async(std::launch::async, [&server] {
        server.accept();
        return true;
    });

    eckit::net::TCPClient client;
    client.connect("localhost", server.localPort());
    accepted.get();

    remote::Connection::setTcpOptions(client);
    remote::Connection::setTcpOptions(server);

    for (int fd : {client.socket(), server.socket()}) {
        EXPECT(getSocketOption(fd, SOL_SOCKET, SO_KEEPALIVE) != 0);
#if defined(TCP_KEEPIDLE) && defined(TCP_KEEPINTVL) && defined(TCP_KEEPCNT)
        EXPECT_EQUAL(getSocketOption(fd, IPPROTO_TCP, TCP_KEEPIDLE), 7);
        EXPECT_EQUAL(getSocketOption(fd, IPPROTO_TCP, TCP_KEEPINTVL), 3);
        EXPECT_EQUAL(getSocketOption(fd, IPPROTO_TCP, TCP_KEEPCNT), 2);
#endif
#ifdef TCP_USER_TIMEOUT
        EXPECT_EQUAL(getSocketOption(fd, IPPROTO_TCP, TCP_USER_TIMEOUT), 11000);
#endif
    }
}

}  // namespace fdb5::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}

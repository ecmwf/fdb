// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// Connection::abortConnection() must release every thread blocked on the connection sockets, in single and dual
// connection mode, even if the connection was already marked invalid by a failed read. This is what allows the
// server to join its listener threads after a failure.

#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/client/Client.h"
#include "fdb5/remote/server/ServerConnection.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/io/Buffer.h"
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPServer.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/testing/Test.h"

#include <poll.h>

#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <memory>
#include <vector>

using namespace eckit::testing;

namespace fdb5::test {

//-----------------------------------------------------------------------------

namespace {

// A socket accepted by the "server" side of the connection, with its peer
struct SocketPair {

    SocketPair() {
        server.socket();  // binds
        auto accepted = std::async(std::launch::async, [this] { server.accept(); });
        client.connect("127.0.0.1", server.localPort());
        accepted.get();
    }

    eckit::net::EphemeralTCPServer server;
    eckit::net::TCPClient client;
};

class TestConnection : public remote::Connection {
public:

    TestConnection(const SocketPair& control, const SocketPair* data) : control_(control), data_(data) {
        single_ = (data == nullptr);
    }

private:

    const eckit::net::TCPSocket& controlSocket() const override { return control_.server; }
    const eckit::net::TCPSocket& dataSocket() const override { return data_->server; }

    const SocketPair& control_;
    const SocketPair* data_;
};

class FailingArchiveConnection : public remote::ServerConnection {
public:

    explicit FailingArchiveConnection(eckit::net::TCPSocket& socket) : ServerConnection(socket, Config{}) {
        single_ = true;
    }

    void enqueue() { queue(remote::Message::Blob, 1, 7, eckit::Buffer{"x", 1}); }
    size_t runArchive() { return archiveThreadLoop(); }

private:

    remote::Handled handleControl(remote::Message, uint32_t, uint32_t) override { return remote::Handled::No; }
    remote::Handled handleControl(remote::Message, uint32_t, uint32_t, eckit::Buffer&&) override {
        return remote::Handled::No;
    }
    void archiveBlob(uint32_t, uint32_t, const void*, size_t) override {
        throw eckit::SeriousBug("synthetic archive failure");
    }
    bool remove(bool, uint32_t) override { return false; }
};

struct BlockingDataState {
    std::promise<void> blocked;
    std::promise<void> release;
    std::future<void> released = release.get_future();
    std::promise<uint32_t> receivedClient;
};

class BlockingDataConnection : public remote::ServerConnection {
public:

    BlockingDataConnection(eckit::net::TCPSocket& socket, const Config& config, BlockingDataState& state) :
        ServerConnection(socket, config), state_(state) {}

    remote::Handled handleData(remote::Message, uint32_t clientID, uint32_t requestID, eckit::Buffer&&) override {
        if (requestID == 100) {
            state_.blocked.set_value();
            state_.released.wait();
        }
        else if (requestID == 102) {
            state_.receivedClient.set_value(clientID);
        }
        return remote::Handled::Replied;
    }

private:

    remote::Handled handleControl(remote::Message message, uint32_t, uint32_t) override {
        return message == remote::Message::Store ? remote::Handled::YesAddArchiveListener : remote::Handled::No;
    }
    remote::Handled handleControl(remote::Message, uint32_t, uint32_t, eckit::Buffer&&) override {
        return remote::Handled::No;
    }
    void archiveBlob(uint32_t, uint32_t, const void*, size_t) override {}
    bool remove(bool, uint32_t) override { return false; }

    BlockingDataState& state_;
};

class TestClient : public remote::Client {
public:

    TestClient(const Config& config, const eckit::net::Endpoint& endpoint) :
        Client(config, endpoint, ""), config_(config) {}
    ~TestClient() override { deregister(); }

    const eckit::Configuration& clientConfig() const override { return config_; }
    bool handle(remote::Message, uint32_t) override { return false; }
    bool handle(remote::Message, uint32_t, eckit::Buffer&&) override { return false; }

private:

    Config config_;
};

remote::Message readUntilReleased(const TestConnection& connection, bool control) {
    remote::MessageHeader hdr{};
    if (control) {
        connection.readControl(hdr);
    }
    else {
        connection.readData(hdr);
    }
    return hdr.message;
}

// A reader that is never released would hang the test, rather than fail it
template <typename T>
T getOrDie(std::future<T>& future) {
    if (future.wait_for(std::chrono::seconds(30)) != std::future_status::ready) {
        std::cerr << "Reader still blocked after abortConnection()" << std::endl;
        std::_Exit(1);
    }
    return future.get();
}

}  // namespace

//-----------------------------------------------------------------------------

CASE("abortConnection releases a reader blocked on the control socket (single connection)") {
    SocketPair control;
    TestConnection connection(control, nullptr);

    auto reader = std::async(std::launch::async, [&] { return readUntilReleased(connection, true); });

    connection.abortConnection();

    EXPECT(getOrDie(reader) == remote::Message::Exit);
    EXPECT(connection.aborted());
}

CASE("abortConnection releases a reader blocked on the data socket (dual connection)") {
    SocketPair control;
    SocketPair data;
    TestConnection connection(control, &data);

    auto dataReader    = std::async(std::launch::async, [&] { return readUntilReleased(connection, false); });
    auto controlReader = std::async(std::launch::async, [&] { return readUntilReleased(connection, true); });

    connection.abortConnection();

    EXPECT(getOrDie(dataReader) == remote::Message::Exit);
    EXPECT(getOrDie(controlReader) == remote::Message::Exit);
}

CASE("abortConnection releases the data reader after the control connection has failed") {
    SocketPair control;
    SocketPair data;
    TestConnection connection(control, &data);

    auto dataReader = std::async(std::launch::async, [&] { return readUntilReleased(connection, false); });

    // a failed read marks the connection as invalid...
    control.client.close();
    remote::MessageHeader hdr{};
    EXPECT_THROWS_AS(connection.readControl(hdr), remote::TCPException);
    EXPECT(!connection.valid());

    // ...which must not prevent the sockets from being shut down
    connection.abortConnection();

    EXPECT(getOrDie(dataReader) == remote::Message::Exit);
}

CASE("abortConnection is idempotent, never reports a graceful Exit and makes writes fail") {
    SocketPair control;
    TestConnection connection(control, nullptr);

    connection.abortConnection();
    connection.abortConnection();
    connection.teardown();  // already aborted: nothing to send

    // the peer sees the connection being lost, with nothing sent
    char buffer[1];
    EXPECT(control.client.read(buffer, sizeof(buffer)) == 0);

    EXPECT_THROWS_AS(connection.write(remote::Message::Blob, true, 1, 1), remote::TCPException);
}

CASE("archive failure aborts even when another writer holds the socket") {
    SocketPair control;
    FailingArchiveConnection connection(control.server);
    std::vector<char> payload(16 * 1024 * 1024, 'x');
    auto writer = std::async(std::launch::async, [&] {
        try {
            connection.write(remote::Message::Blob, true, 1, 2, payload.data(), payload.size());
            return false;
        }
        catch (const remote::TCPException&) {
            return true;
        }
    });

    pollfd peer{control.client.socket(), POLLIN, 0};
    if (::poll(&peer, 1, 10000) <= 0 ||
        writer.wait_for(std::chrono::milliseconds(200)) != std::future_status::timeout) {
        connection.abortConnection();
        getOrDie(writer);
        throw eckit::SeriousBug("Failed to block the test socket writer");
    }

    connection.enqueue();
    auto archive = std::async(std::launch::async, [&] { return connection.runArchive(); });
    EXPECT_THROWS_AS(getOrDie(archive), eckit::SeriousBug);
    EXPECT(getOrDie(writer));
}

CASE("queued data retains its client ID after the client is destroyed") {
    eckit::net::TCPServer listener;
    listener.socket();
    Config config;
    config.set("supportedConnections", std::vector<int>{2});
    BlockingDataState state;

    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        BlockingDataConnection connection(socket, config, state);
        connection.handle();
        return true;
    });

    eckit::net::Endpoint endpoint("127.0.0.1", listener.localPort());
    auto client = std::make_unique<TestClient>(config, endpoint);
    auto keeper = std::make_unique<TestClient>(config, endpoint);
    client->controlWriteCheckResponse(remote::Message::Store, client->generateRequestID(), true);

    const char byte = 'x';
    client->dataWrite(remote::Message::Blob, 100, {{1, &byte}});
    auto blocked = state.blocked.get_future();
    getOrDie(blocked);

    std::vector<char> payload(16 * 1024 * 1024, 'x');
    client->dataWrite(remote::Message::Blob, 101, {{payload.size(), payload.data()}});
    client->dataWrite(remote::Message::Blob, 102, {{1, &byte}});
    auto clientID = client->id();
    client.reset();

    auto received = state.receivedClient.get_future();
    state.release.set_value();
    EXPECT(getOrDie(received) == clientID);

    keeper.reset();
    EXPECT(getOrDie(server));
}

}  // namespace fdb5::test

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}

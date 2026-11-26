// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// Connection::abortConnection() must release every thread blocked on the connection sockets, in single and dual
// connection mode, even if the connection was already marked invalid by a failed read. This is what allows the
// server to join its listener threads after a failure.

#include "RemoteStoreTestHarness.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Key.h"
#include "fdb5/remote/Connection.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/RemoteConfiguration.h"
#include "fdb5/remote/client/Client.h"
#include "fdb5/remote/client/ClientConnection.h"
#include "fdb5/remote/client/ReadLimiter.h"
#include "fdb5/remote/client/RemoteCatalogue.h"
#include "fdb5/remote/client/RemoteStore.h"
#include "fdb5/remote/client/RequestQueues.h"
#include "fdb5/remote/server/ServerConnection.h"
#include "fdb5/toc/TocFieldLocation.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Buffer.h"
#include "eckit/net/TCPClient.h"
#include "eckit/net/TCPServer.h"
#include "eckit/net/TCPSocket.h"
#include "eckit/runtime/SessionID.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/testing/Test.h"

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
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

    void limitPayload(uint32_t size) { maxPayloadSize(size); }

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

    // What a listener does when it fails: try to take the reporting of the failure
    bool failAsListener() { return failConnection("listener failure", 0, 0); }

private:

    remote::Handled handleControl(remote::Message, uint32_t, uint32_t) override { return remote::Handled::No; }
    remote::Handled handleControl(remote::Message, uint32_t, uint32_t, eckit::Buffer&&) override {
        return remote::Handled::No;
    }
    void archiveBlob(uint32_t, uint32_t, const void*, size_t) override {
        throw eckit::SeriousBug("synthetic archive failure");
    }
    void remove(uint32_t) override {}
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
    void remove(uint32_t) override {}

    BlockingDataState& state_;
};

class TestClient : public remote::Client {
public:

    TestClient(const Config& config, const eckit::net::Endpoint& endpoint, const std::string& defaultEndpoint = "") :
        Client(config, endpoint, defaultEndpoint), config_(config) {}

    // with failover between the endpoints (as an archiving RemoteStore)
    TestClient(const Config& config, const EndpointList& endpoints) : Client(config, endpoints), config_(config) {}
    ~TestClient() override { deregister(); }

    const void* connectionAddress() const { return connection_.get(); }

    // waits until the queued data writes have been sent (or failed)
    void flushData() { connection_->flushDataWrites(); }

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


// A catalogue server whose archive worker fails. Optionally, the failure waits until it is released, so that the
// client can fill the archive queue meanwhile and the data listener is blocked on it when the worker fails.
struct FailingCatalogueState {
    std::atomic<bool> holdFailure{false};
    std::promise<void> failing;
    std::promise<void> release;
    std::shared_future<void> released = release.get_future().share();
};

class FailingCatalogueConnection : public remote::ServerConnection {
public:

    FailingCatalogueConnection(eckit::net::TCPSocket& socket, const Config& config, FailingCatalogueState& state) :
        ServerConnection(socket, config), state_(state) {}

    ~FailingCatalogueConnection() override { stopWorkers(true); }

private:

    remote::Handled handleControl(remote::Message message, uint32_t, uint32_t) override {
        if (message == remote::Message::Archive) {
            archiver();
            return remote::Handled::YesAddArchiveListener;
        }
        return remote::Handled::No;
    }
    remote::Handled handleControl(remote::Message, uint32_t, uint32_t, eckit::Buffer&&) override {
        return remote::Handled::No;
    }
    void archiveBlob(uint32_t, uint32_t, const void*, size_t) override {
        if (state_.holdFailure) {
            std::call_once(failingOnce_, [this] { state_.failing.set_value(); });
            state_.released.wait();
        }
        throw eckit::SeriousBug("synthetic catalogue archive failure");
    }
    void remove(uint32_t) override {}

    FailingCatalogueState& state_;
    std::once_flag failingOnce_;
};

// Gives access to the state of the connection of a RemoteCatalogue
class TestCatalogue : public remote::RemoteCatalogue {
public:

    using RemoteCatalogue::RemoteCatalogue;

    bool connectionValid() const { return connection_->valid(); }
};

Key catalogueKey() {
    Key key;
    key.set("class", "od");
    key.set("expver", "xxxx");
    return key;
}

void archiveOneLocation(remote::RemoteCatalogue& catalogue, size_t i) {
    Key datumKey;
    datumKey.set("step", std::to_string(i));
    catalogue.archive(Key{}, datumKey, std::make_shared<TocFieldLocation>(eckit::PathName("/dev/null"), i, 1, Key{}));
}

// Waits until the client has seen the server abort the connection, i.e. after it has processed the error
void waitForLostConnection(const TestCatalogue& catalogue) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (catalogue.connectionValid()) {
        if (std::chrono::steady_clock::now() > deadline) {
            std::cerr << "The server did not abort the connection" << std::endl;
            std::_Exit(1);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

std::string flushError(remote::RemoteCatalogue& catalogue, size_t archived) {
    try {
        catalogue.flush(archived);
    }
    catch (const std::exception& e) {
        return e.what();
    }
    return "flush() succeeded";
}

Config catalogueClientConfig(int port) {
    Config config;
    config.set("host", "127.0.0.1");
    config.set("port", port);
    config.set("supportedConnections", std::vector<int>{2});
    return config;
}


// A server that terminates the connection with a connection-level error, i.e. not addressed to any request, on demand
class FailingOnDemandConnection : public remote::ServerConnection {
public:

    FailingOnDemandConnection(eckit::net::TCPSocket& socket, const Config& config) : ServerConnection(socket, config) {}

    void fail(const std::string& reason) { failConnection(reason, 0, 0); }

    // a connection-level error (clientID 0), leaving the connection open
    void sendConnectionError(const std::string& reason) { tryError(true, reason, 0, 0); }

    // a graceful end of the connection by the server (Exit)
    void end() { teardown(); }

    // the server ends the control channel only, and leaves the data channel open (silent)
    void endControlOnly() { write(remote::Message::Exit, true, 0, 0); }

    // the data connection fails, the control connection stays healthy (and silent)
    void failDataOnly() { shutdownSocket(false); }

private:

    remote::Handled handleControl(remote::Message, uint32_t, uint32_t) override { return remote::Handled::No; }
    remote::Handled handleControl(remote::Message, uint32_t, uint32_t, eckit::Buffer&&) override {
        return remote::Handled::No;
    }
    void archiveBlob(uint32_t, uint32_t, const void*, size_t) override {}
    void remove(uint32_t) override {}
};


// A catalogue server whose failure is first seen by the client on the data socket: it shuts the data socket down,
// and only later sends the error of the failing archive request on the control socket, then aborts. This is the
// order in which the client may observe a real failure, as its threads race to read the two sockets.
struct DataFirstFailureState {
    std::promise<std::pair<uint32_t, uint32_t>> archived;  // clientID, requestID of the catalogue blob
    std::promise<void> blocked;
    std::promise<void> release;
    std::shared_future<void> released = release.get_future().share();
};

class DataFirstFailureConnection : public remote::ServerConnection {
public:

    static constexpr uint32_t blockingRequest = 100;

    DataFirstFailureConnection(eckit::net::TCPSocket& socket, const Config& config, DataFirstFailureState& state) :
        ServerConnection(socket, config), state_(state) {}

    void failDataFirst(const std::string& reason, uint32_t clientID, uint32_t requestID) {
        shutdownSocket(false);
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        tryError(true, reason, clientID, requestID);
        abortConnection();
    }

private:

    remote::Handled handleControl(remote::Message message, uint32_t, uint32_t) override {
        return message == remote::Message::Archive ? remote::Handled::YesAddArchiveListener : remote::Handled::No;
    }
    remote::Handled handleControl(remote::Message, uint32_t, uint32_t, eckit::Buffer&&) override {
        return remote::Handled::No;
    }
    remote::Handled handleData(remote::Message, uint32_t clientID, uint32_t requestID, eckit::Buffer&&) override {
        if (requestID == blockingRequest) {
            // stop reading the data socket, so that the writes of the client pile up behind it
            state_.blocked.set_value();
            state_.released.wait();
        }
        else if (!archivedSet_) {
            archivedSet_ = true;
            state_.archived.set_value({clientID, requestID});
        }
        return remote::Handled::Replied;
    }
    void archiveBlob(uint32_t, uint32_t, const void*, size_t) override {}
    void remove(uint32_t) override {}

    DataFirstFailureState& state_;
    bool archivedSet_{false};
};


// Records the blobs it archives: the parsing of the incoming messages is tested, not the archival
class RecordingArchiveConnection : public remote::ServerConnection {
public:

    explicit RecordingArchiveConnection(eckit::net::TCPSocket& socket) : ServerConnection(socket, Config{}) {
        single_ = true;
    }

    void enqueue(remote::Message message, eckit::Buffer&& payload) { queue(message, 1, 7, std::move(payload)); }
    void endOfArchive() { archiveQueue_.close(); }
    size_t runArchive() { return archiveThreadLoop(); }

    std::vector<std::string> archived;

private:

    remote::Handled handleControl(remote::Message, uint32_t, uint32_t) override { return remote::Handled::No; }
    remote::Handled handleControl(remote::Message, uint32_t, uint32_t, eckit::Buffer&&) override {
        return remote::Handled::No;
    }
    void archiveBlob(uint32_t, uint32_t, const void* data, size_t length) override {
        archived.emplace_back(static_cast<const char*>(data), length);
    }
    void remove(uint32_t) override {}
};

// Frames for a MultiBlob: [header, payload, end marker]...
void appendFrame(std::string& out, uint32_t clientID, uint32_t requestID, const std::string& payload,
                 uint32_t declaredSize) {
    const remote::MessageHeader hdr{remote::Message::Blob, false, clientID, requestID, declaredSize};
    out.append(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
    out.append(payload);
    out.append(remote::MessageHeader::EndMarker.data(), remote::MessageHeader::markerBytes);
}

// A store server that acknowledges the first reads, and rejects the following ones (without closing the connection)
struct ReadServerState {
    std::atomic<size_t> reads{0};
    size_t accepted{0};
};

class ReadServerConnection : public remote::ServerConnection {
public:

    ReadServerConnection(eckit::net::TCPSocket& socket, const Config& config, ReadServerState& state) :
        ServerConnection(socket, config), state_(state) {}

private:

    remote::Handled handleControl(remote::Message, uint32_t, uint32_t) override { return remote::Handled::No; }
    remote::Handled handleControl(remote::Message message, uint32_t, uint32_t, eckit::Buffer&&) override {
        if (message == remote::Message::Read) {
            // n.b. no data is sent: the reads stay active until their handles are destroyed
            return ++state_.reads <= state_.accepted ? remote::Handled::Yes : remote::Handled::No;
        }
        return remote::Handled::No;
    }
    void archiveBlob(uint32_t, uint32_t, const void*, size_t) override {}
    void remove(uint32_t) override {}

    ReadServerState& state_;
};

// Sets the memory limit of the reads of the process for one test
struct ReadLimit {
    explicit ReadLimit(size_t limit) { remote::ReadLimiter::instance().setMemoryLimit(limit); }
    ~ReadLimit() { remote::ReadLimiter::instance().setMemoryLimit(size_t{1024} * 1024 * 1024); }
};

std::unique_ptr<eckit::DataHandle> readField(remote::RemoteStore& store, size_t length) {
    return std::unique_ptr<eckit::DataHandle>(
        store.dataHandle(TocFieldLocation(eckit::PathName("/dev/null"), 0, length, Key{})));
}

// A client that blocks while it is told that the connection is lost, which keeps the connection in the state where the
// pending requests have been failed, but the connection is not yet marked invalid
class BlockingCloseClient : public TestClient {
public:

    using TestClient::TestClient;
    ~BlockingCloseClient() override { deregister(); }

    std::promise<void> entered;
    std::promise<void> release;

private:

    void closeConnection(const std::exception_ptr&) override {
        if (!blocked_.exchange(true)) {
            entered.set_value();
            release.get_future().wait();
        }
    }

    std::atomic<bool> blocked_{false};
};


// Serves the given number of connections, each with a FailingOnDemandConnection (which handles nothing)
class MultiServer {
public:

    MultiServer(const Config& config, size_t connections) : config_(config), connections_(connections) {
        listener_.socket();
        thread_ = std::thread([this, connections] {
            std::vector<std::thread> handlers;
            for (size_t i = 0; i < connections; ++i) {
                auto accepted = std::make_shared<eckit::net::TCPSocket>(listener_.accept());
                ++accepted_;
                handlers.emplace_back([this, accepted] {
                    FailingOnDemandConnection connection(*accepted, config_);
                    connection.handle();
                });
            }
            for (auto& h : handlers) {
                h.join();
            }
        });
    }

    ~MultiServer() {
        // a test that failed early may not have made all the connections: unblock the server with empty ones
        for (size_t i = accepted_; i < connections_; ++i) {
            try {
                eckit::net::TCPClient unblock;
                unblock.connect("127.0.0.1", listener_.localPort());
            }
            catch (...) {
            }
        }
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    eckit::net::Endpoint endpoint() { return {"127.0.0.1", listener_.localPort()}; }

private:

    Config config_;
    size_t connections_;
    std::atomic<size_t> accepted_{0};
    eckit::net::TCPServer listener_;
    std::thread thread_;
};

// Sets an environment variable for the duration of a test
struct ScopedEnv {
    ScopedEnv(const char* name, const char* value) : name_(name) { ::setenv(name, value, 1); }
    ~ScopedEnv() { ::unsetenv(name_); }
    const char* name_;
};

// Number of open file descriptors of the process
size_t openDescriptors() {
    size_t count = 0;
    for (int fd = 0; fd < 4096; ++fd) {
        if (::fcntl(fd, F_GETFD) != -1) {
            ++count;
        }
    }
    return count;
}


// A handler that starts its archive worker, and does not stop it in its destructor: a bug
class MisbehavingConnection : public remote::ServerConnection {
public:

    explicit MisbehavingConnection(eckit::net::TCPSocket& socket) : ServerConnection(socket, Config{}) {
        single_ = true;
        archiver();  // the worker waits on the archive queue
    }

private:

    remote::Handled handleControl(remote::Message, uint32_t, uint32_t) override { return remote::Handled::No; }
    remote::Handled handleControl(remote::Message, uint32_t, uint32_t, eckit::Buffer&&) override {
        return remote::Handled::No;
    }
    void archiveBlob(uint32_t, uint32_t, const void*, size_t) override {}
    void remove(uint32_t) override {}
};


// A failure whose description takes a while: it keeps open any window between the end of the element and the record
// of its failure
class SlowDescriptionFailure : public std::exception {
public:

    const char* what() const noexcept override {
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        return "synthetic failure of the last location";
    }
};

class FailingLastLocationConnection : public remote::ServerConnection {
public:

    explicit FailingLastLocationConnection(eckit::net::TCPSocket& socket) : ServerConnection(socket, Config{}) {
        single_ = true;
    }

    void enqueue() { queue(remote::Message::Blob, 1, 7, eckit::Buffer{"x", 1}); }
    size_t runArchive() { return archiveThreadLoop(); }
    size_t pending() const { return archivePending(); }
    bool failureRecorded() const {
        try {
            rethrowArchiveFailure();
            return false;
        }
        catch (...) {
            return true;
        }
    }

private:

    remote::Handled handleControl(remote::Message, uint32_t, uint32_t) override { return remote::Handled::No; }
    remote::Handled handleControl(remote::Message, uint32_t, uint32_t, eckit::Buffer&&) override {
        return remote::Handled::No;
    }
    void archiveBlob(uint32_t, uint32_t, const void*, size_t) override { throw SlowDescriptionFailure(); }
    void remove(uint32_t) override {}
};


// A server that accepts archive data and discards it, and can end the connection gracefully
class DiscardingConnection : public remote::ServerConnection {
public:

    DiscardingConnection(eckit::net::TCPSocket& socket, const Config& config) : ServerConnection(socket, config) {}

    void end() { teardown(); }

    remote::Handled handleData(remote::Message, uint32_t, uint32_t, eckit::Buffer&&) override {
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
    void remove(uint32_t) override {}
};


// Pauses the next data write once it holds the queue of the writer, and before it queues its request (see
// ClientConnection::setDataWriteHook()), until it is released. The hook is removed when the gate is destroyed.
class WriteGate {
public:

    WriteGate() {
        std::lock_guard lock(mutex_);
        paused_ = false;
        released_ = false;
        remote::ClientConnection::setDataWriteHook(&WriteGate::hook);
    }

    WriteGate(const WriteGate&) = delete;
    WriteGate& operator=(const WriteGate&) = delete;

    ~WriteGate() {
        release();
        remote::ClientConnection::setDataWriteHook(nullptr);
    }

    // @returns true once a write is paused
    bool awaitPaused(std::chrono::seconds timeout) {
        std::unique_lock lock(mutex_);
        return condition_.wait_for(lock, timeout, [] { return paused_; });
    }

    void release() {
        {
            std::lock_guard lock(mutex_);
            released_ = true;
        }
        condition_.notify_all();
    }

private:

    static void hook() {
        std::unique_lock lock(mutex_);
        paused_ = true;
        condition_.notify_all();
        condition_.wait(lock, [] { return released_; });
    }

    static inline std::mutex mutex_;
    static inline std::condition_variable condition_;
    static inline bool paused_ = false;
    static inline bool released_ = false;
};

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

    auto dataReader = std::async(std::launch::async, [&] { return readUntilReleased(connection, false); });
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

CASE("a thread woken up by the failure of the archive worker neither preempts nor replaces its error") {
    SocketPair control;
    FailingArchiveConnection connection(control.server);

    // Fill the archive queue (fdbServerMaxQueueSize), so that the next producer blocks on it, as the data listener
    // does when the worker is slower than the client
    constexpr size_t queueSize = 320;
    for (size_t i = 0; i < queueSize; ++i) {
        connection.enqueue();
    }

    auto listener = std::async(std::launch::async, [&] {
        try {
            // keeps producing, as the data listener does: the queue is full, so it is blocked when the worker fails
            for (size_t i = 0; i < 10 * queueSize; ++i) {
                connection.enqueue();
            }
            return std::string("enqueue() never interrupted");
        }
        catch (const std::exception&) {
            // By the time the interruption wakes up a producer, the worker has already sent its error and aborted
            // the connection: nothing the producer does can preempt that error
            if (!connection.aborted()) {
                return std::string("woken up before the worker reported its failure");
            }
            // the failure of the listener is a consequence: it must not own the report
            return std::string(connection.failAsListener() ? "listener owned the failure" : "worker owned the failure");
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT(listener.wait_for(std::chrono::seconds(0)) == std::future_status::timeout);

    auto archive = std::async(std::launch::async, [&] { return connection.runArchive(); });
    EXPECT_THROWS_AS(getOrDie(archive), eckit::SeriousBug);
    const auto owner = getOrDie(listener);  // n.b. not in EXPECT_EQUAL, which evaluates it again on failure
    EXPECT_EQUAL(owner, std::string("worker owned the failure"));
    EXPECT(connection.aborted());

    // The peer gets the error of the failing request, and then nothing but the loss of the connection
    remote::MessageHeader hdr{};
    EXPECT(control.client.read(&hdr, sizeof(hdr)) == static_cast<long>(sizeof(hdr)));
    EXPECT(hdr.message == remote::Message::Error);
    EXPECT_EQUAL(hdr.clientID(), 1u);
    EXPECT_EQUAL(hdr.requestID, 7u);

    std::string received(hdr.payloadSize, ' ');
    EXPECT(control.client.read(received.data(), received.size()) == static_cast<long>(received.size()));
    EXPECT(received.find("synthetic archive failure") != std::string::npos);

    char marker[remote::MessageHeader::markerBytes];
    EXPECT(control.client.read(marker, sizeof(marker)) == static_cast<long>(sizeof(marker)));
    char extra[1];
    EXPECT(control.client.read(extra, sizeof(extra)) == 0);
}

CASE("tryErrorBounded delivers the error, and gives up after a bounded wait for another writer") {
    SocketPair control;
    TestConnection connection(control, nullptr);

    const std::string reason = "synthetic failure";
    EXPECT(connection.tryErrorBounded(true, reason, 3, 4));

    remote::MessageHeader hdr{};
    EXPECT(control.client.read(&hdr, sizeof(hdr)) == static_cast<long>(sizeof(hdr)));
    EXPECT(hdr.message == remote::Message::Error);
    EXPECT_EQUAL(hdr.clientID(), 3u);
    EXPECT_EQUAL(hdr.requestID, 4u);
    EXPECT_EQUAL(hdr.payloadSize, reason.size());

    std::string received(hdr.payloadSize, ' ');
    EXPECT(control.client.read(received.data(), received.size()) == static_cast<long>(received.size()));
    EXPECT_EQUAL(received, reason);

    // a writer that never drains its socket must not block the error
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
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT(writer.wait_for(std::chrono::seconds(0)) == std::future_status::timeout);

    EXPECT(!connection.tryErrorBounded(true, reason, 3, 4));

    connection.abortConnection();
    EXPECT(getOrDie(writer));
}

CASE("tryErrorBounded does not block when the peer has stopped reading and the send buffer is full") {
    SocketPair control;
    TestConnection connection(control, nullptr);

    // fill the send buffer without holding the mutex of the connection: only the socket can block
    const int fd = control.server.eckit::net::TCPSocket::socket();
    const timeval fillTimeout{0, 50 * 1000};
    EXPECT(::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &fillTimeout, sizeof(fillTimeout)) == 0);

    // shrinking chunks, so that not even a few bytes are left: a short send leaves some room in the buffer
    const std::vector<char> filler(64 * 1024, 'x');
    for (size_t chunk : {filler.size(), size_t{512}, size_t{1}}) {
        while (::send(fd, filler.data(), chunk, 0) == static_cast<long>(chunk)) {}
    }

    const auto start = std::chrono::steady_clock::now();
    auto attempt = std::async(std::launch::async, [&] { return connection.tryErrorBounded(true, "failure", 3, 4); });
    EXPECT(getOrDie(attempt) == false);
    EXPECT(std::chrono::steady_clock::now() - start < std::chrono::seconds(5));
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


CASE("a catalogue archive failure reported before flush() is rethrown by flush()") {
    eckit::net::TCPServer listener;
    listener.socket();
    const Config config = catalogueClientConfig(listener.localPort());
    FailingCatalogueState state;

    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        FailingCatalogueConnection connection(socket, config, state);
        connection.handle();
        return true;
    });

    {
        TestCatalogue catalogue(catalogueKey(), config);
        archiveOneLocation(catalogue, 0);

        // The error is not the answer to any pending request: the server sends it and then aborts the connection,
        // all before flush() registers its request
        waitForLostConnection(catalogue);

        const auto error = flushError(catalogue, 1);
        eckit::Log::info() << "flush() failed with: " << error << std::endl;
        EXPECT(error.find("synthetic catalogue archive failure") != std::string::npos);

        // a retry (once the store has reset its count) reports the same failure, not a count mismatch
        EXPECT(flushError(catalogue, 0).find("synthetic catalogue archive failure") != std::string::npos);

        // and archiving more fails fast, with the same reason
        std::string archiveError;
        try {
            archiveOneLocation(catalogue, 1);
        }
        catch (const std::exception& e) {
            archiveError = e.what();
        }
        EXPECT(archiveError.find("synthetic catalogue archive failure") != std::string::npos);
    }

    EXPECT(getOrDie(server));
}

CASE("the archive worker reports its error even if its failure wakes up the data listener") {
    eckit::net::TCPServer listener;
    listener.socket();
    const Config config = catalogueClientConfig(listener.localPort());
    FailingCatalogueState state;
    state.holdFailure = true;

    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        FailingCatalogueConnection connection(socket, config, state);
        connection.handle();
        return true;
    });

    {
        TestCatalogue catalogue(catalogueKey(), config);

        // More locations than the archive queue holds (fdbServerMaxQueueSize): once the worker is stuck on the first
        // one, the data listener blocks on the full queue, and the failure of the worker interrupts it
        constexpr size_t numLocations = 400;
        for (size_t i = 0; i < numLocations; ++i) {
            archiveOneLocation(catalogue, i);
        }

        auto failing = state.failing.get_future();
        getOrDie(failing);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));  // let the data listener fill the queue
        state.release.set_value();

        waitForLostConnection(catalogue);

        const auto error = flushError(catalogue, numLocations);
        eckit::Log::info() << "flush() failed with: " << error << std::endl;
        EXPECT(error.find("synthetic catalogue archive failure") != std::string::npos);
        EXPECT(error.find("Data listener failed") == std::string::npos);
    }

    EXPECT(getOrDie(server));
}


CASE("a read waiting after its acknowledgement gets the reason of a connection-level server error") {
    eckit::net::TCPServer listener;
    listener.socket();
    const Config config = catalogueClientConfig(listener.localPort());

    std::promise<FailingOnDemandConnection*> started;
    std::promise<void> failed;
    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        FailingOnDemandConnection connection(socket, config);
        started.set_value(&connection);
        connection.handle();
        failed.get_future().wait();  // fail() is still using the connection
        return true;
    });

    {
        remote::RemoteStore store(eckit::URI("fdb://127.0.0.1:" + std::to_string(listener.localPort())), config);
        auto queue = remote::RemoteStoreTestHarness::registerQueue(store).second;

        auto reader = std::async(std::launch::async, [queue] {
            remote::RemoteStore::StoredMessage msg;
            try {
                queue->pop(msg);
                return std::string("pop() returned");
            }
            catch (const std::exception& e) {
                return std::string(e.what());
            }
        });

        auto connection = started.get_future();
        getOrDie(connection)->fail("synthetic connection failure");
        failed.set_value();

        const auto error = getOrDie(reader);
        eckit::Log::info() << "reader woken with: " << error << std::endl;
        EXPECT(error.find("Unexpected closure of store") != std::string::npos);
        EXPECT(error.find("synthetic connection failure") != std::string::npos);
    }

    EXPECT(getOrDie(server));
}


// Runs a flush() that is blocked behind the writes of another client when the server fails: the server shuts the
// data socket down first, and sends its error on the control socket only later. Returns what flush() threw, and what
// a retry of the same cycle throws (as the destructor of the archiver does, once the store has reset its count).
std::pair<std::string, std::string> flushFailingDataFirst(const std::string& reason, bool requestScoped) {
    eckit::net::TCPServer listener;
    listener.socket();
    const Config config = catalogueClientConfig(listener.localPort());
    DataFirstFailureState state;

    std::promise<DataFirstFailureConnection*> started;
    std::promise<void> failed;
    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        DataFirstFailureConnection connection(socket, config, state);
        started.set_value(&connection);
        connection.handle();
        failed.get_future().wait();  // failDataFirst() is still using the connection
        return true;
    });
    auto connection = started.get_future();

    std::pair<std::string, std::string> errors;
    {
        TestCatalogue catalogue(catalogueKey(), config);
        archiveOneLocation(catalogue, 0);
        auto archived = state.archived.get_future();
        const auto [clientID, requestID] = getOrDie(archived);

        // Another client of the same connection makes the server stop reading the data socket, then fills it: the
        // barrier of the flush is queued behind its writes
        eckit::net::Endpoint endpoint("127.0.0.1", listener.localPort());
        TestClient other(config, endpoint);
        const char byte = 'x';
        other.dataWrite(remote::Message::Blob, DataFirstFailureConnection::blockingRequest, {{1, &byte}});
        auto blocked = state.blocked.get_future();
        getOrDie(blocked);
        std::vector<char> payload(16 * 1024 * 1024, 'x');
        other.dataWrite(remote::Message::Blob, DataFirstFailureConnection::blockingRequest + 1,
                        {{payload.size(), payload.data()}});

        auto flush = std::async(std::launch::async, [&] { return flushError(catalogue, 1); });
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        EXPECT(flush.wait_for(std::chrono::seconds(0)) == std::future_status::timeout);  // flush() is in progress

        // a request-scoped error is addressed to the failing archive request, a connection-level one to nobody
        getOrDie(connection)->failDataFirst(reason, requestScoped ? clientID : 0, requestScoped ? requestID : 0);
        state.release.set_value();
        failed.set_value();

        errors.first = getOrDie(flush);
        errors.second = flushError(catalogue, 0);
    }

    EXPECT(getOrDie(server));
    return errors;
}

CASE("flush() in progress reports the archive error even if it sees the data socket fail first") {
    const std::string reason = "synthetic archive failure, reported after the data socket";
    const auto [error, retry] = flushFailingDataFirst(reason, true);
    eckit::Log::info() << "flush() failed with: " << error << std::endl;
    EXPECT(error.find(reason) != std::string::npos);
    EXPECT(retry.find(reason) != std::string::npos);
}

CASE("flush() in progress reports a connection-level server error even if it sees the data socket fail first") {
    const std::string reason = "synthetic connection failure, reported after the data socket";
    const auto [error, retry] = flushFailingDataFirst(reason, false);
    eckit::Log::info() << "flush() failed with: " << error << std::endl;
    EXPECT(error.find(reason) != std::string::npos);

    // the retry of the cycle reports the same failure, not a mismatch between the counts of store and catalogue
    eckit::Log::info() << "retry of flush() failed with: " << retry << std::endl;
    EXPECT(retry.find(reason) != std::string::npos);
}

// The server accepts the data connection asynchronously, after the client has connected: before the test writes on
// it, make sure the server serves the connection, with a request (rejected) that is answered only once it does
void waitForServer(remote::Client& client) {
    try {
        client.controlWriteCheckResponse(remote::Message::Exists, client.generateRequestID(), false);
    }
    catch (const std::exception&) {
        // the test servers do not handle it: the answer is all that matters
    }
}

// A read whose consumer has paused fills its queue (320 messages), and the data listener of the client blocks on the
// next message of the read: a chunk (Blob), or the end of the read (Complete).
void fillPausedRead(remote::ServerConnection& server, remote::RemoteStore& store, uint32_t requestID,
                    remote::Message last) {
    waitForServer(store);

    constexpr size_t queueSize = 320;
    const char byte = 'x';
    for (size_t i = 0; i < queueSize; ++i) {
        server.write(remote::Message::Blob, false, store.id(), requestID, &byte, 1);
    }
    if (last == remote::Message::Complete) {
        server.write(remote::Message::Complete, false, store.id(), requestID);
    }
    else {
        server.write(remote::Message::Blob, false, store.id(), requestID, &byte, 1);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));  // let the data listener block on the full queue
}

std::future<std::string> startStoreReader(std::shared_ptr<remote::RemoteStore::MessageQueue> queue) {
    return std::async(std::launch::async, [queue] {
        remote::RemoteStore::StoredMessage msg;
        try {
            queue->pop(msg);
            return std::string("pop() returned");
        }
        catch (const std::exception& e) {
            return std::string(e.what());
        }
    });
}

void testFailureWithBlockedListener(remote::Message last) {
    eckit::net::TCPServer listener;
    listener.socket();
    const Config config = catalogueClientConfig(listener.localPort());

    std::promise<FailingOnDemandConnection*> started;
    std::promise<void> failed;
    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        FailingOnDemandConnection connection(socket, config);
        started.set_value(&connection);
        connection.handle();
        failed.get_future().wait();  // fail() is still using the connection
        return true;
    });

    {
        remote::RemoteStore store(eckit::URI("fdb://127.0.0.1:" + std::to_string(listener.localPort())), config);

        // the server connection exists once the client has connected
        auto startedFuture = started.get_future();
        FailingOnDemandConnection* connection = getOrDie(startedFuture);

        // Another request on the same connection, waiting
        auto waiting = remote::RemoteStoreTestHarness::registerQueue(store).second;
        auto reader = startStoreReader(waiting);

        auto [pausedID, paused] = remote::RemoteStoreTestHarness::registerQueue(store);
        fillPausedRead(*connection, store, pausedID, last);
        EXPECT(reader.wait_for(std::chrono::seconds(0)) == std::future_status::timeout);

        connection->fail("synthetic connection failure");
        failed.set_value();

        // the waiting request is woken up with the reason...
        const auto error = getOrDie(reader);
        eckit::Log::info() << "waiting reader woken with: " << error << std::endl;
        EXPECT(error.find("synthetic connection failure") != std::string::npos);

        // ...and the paused one gets the failure as soon as it reads again, instead of the remaining chunks.
        // n.b. the requests are failed one after the other (by requestID), so the waiting one may be woken up before
        // the paused one is failed: until then, a read still returns the chunks queued before the failure
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (!paused->closed() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        EXPECT(paused->closed());

        std::string pausedError;
        try {
            remote::RemoteStore::StoredMessage msg;
            paused->pop(msg);
        }
        catch (const std::exception& e) {
            pausedError = e.what();
        }
        EXPECT(pausedError.find("synthetic connection failure") != std::string::npos);
    }

    EXPECT(getOrDie(server));
}

CASE("connection failure wakes up the requests of a connection whose listener is blocked on a full read queue") {
    testFailureWithBlockedListener(remote::Message::Blob);
}

CASE("connection failure wakes up the requests of a connection whose listener is blocked queueing the end of a read") {
    testFailureWithBlockedListener(remote::Message::Complete);
}

CASE("a client removed while its listener call is blocked on a full read queue is released, the connection is not") {
    eckit::net::TCPServer listener;
    listener.socket();
    const Config config = catalogueClientConfig(listener.localPort());
    const eckit::URI uri("fdb://127.0.0.1:" + std::to_string(listener.localPort()));

    // The test uses the server connection until the end, also after a failure: it is destroyed only then
    std::promise<void> finished;
    struct Finish {
        explicit Finish(std::promise<void>& p) : finished(&p) {}
        Finish(const Finish&) = delete;
        Finish& operator=(const Finish&) = delete;
        ~Finish() { now(); }
        void now() {
            if (finished) {
                finished->set_value();
                finished = nullptr;
            }
        }
        std::promise<void>* finished;
    };

    std::promise<FailingOnDemandConnection*> started;
    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        FailingOnDemandConnection connection(socket, config);
        started.set_value(&connection);
        connection.handle();
        finished.get_future().wait();
        return true;
    });
    Finish finish(finished);  // also if the test fails before the end

    // keeps the connection open, and checks that it is still served once the other store is gone
    remote::RemoteStore keeper(uri, config);
    auto startedFuture = started.get_future();
    FailingOnDemandConnection* connection = getOrDie(startedFuture);

    auto store = std::make_unique<remote::RemoteStore>(uri, config);
    auto pausedID = remote::RemoteStoreTestHarness::registerQueue(*store).first;
    fillPausedRead(*connection, *store, pausedID, remote::Message::Blob);

    // The store goes away while the call of the data listener is blocked on its queue: nobody will ever read it, so
    // its requests must be cancelled for the call, and the removal, to complete
    const auto removedID = store->id();
    auto removed = std::async(std::launch::async, [&store] {
        store.reset();
        return true;
    });
    EXPECT(getOrDie(removed));

    // the server may still be sending for the removed store: it is discarded, it does not fail the connection
    const char late = 'l';
    for (size_t i = 0; i < 3; ++i) {
        connection->write(remote::Message::Blob, false, removedID, pausedID, &late, 1);
    }
    connection->write(remote::Message::Complete, false, removedID, pausedID);

    // the connection still serves the other store
    auto [keeperID, keeperQueue] = remote::RemoteStoreTestHarness::registerQueue(keeper);
    auto reader = startStoreReader(keeperQueue);
    const char byte = 'k';
    connection->write(remote::Message::Blob, false, keeper.id(), keeperID, &byte, 1);
    const auto read = getOrDie(reader);  // n.b. not in EXPECT_EQUAL, which evaluates it again on failure
    eckit::Log::info() << "read on the other store: " << read << std::endl;
    EXPECT_EQUAL(read, std::string("pop() returned"));

    connection->fail("end of test");
    finish.now();
    EXPECT(getOrDie(server));
}

CASE("a client rejected during the startup handshake gets the reason of the server") {
    eckit::net::TCPServer listener;
    listener.socket();

    Config serverConfig;
    serverConfig.set("supportedConnections", std::vector<int>{2});

    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        FailingOnDemandConnection connection(socket, serverConfig);
        connection.handle();
        return true;
    });

    // no number of connections in common with the server: rejected
    Config clientConfig;
    clientConfig.set("supportedConnections", std::vector<int>{1});

    std::string error;
    try {
        TestClient client(clientConfig, eckit::net::Endpoint("127.0.0.1", listener.localPort()));
    }
    catch (const std::exception& e) {
        error = e.what();
    }
    eckit::Log::info() << "connection failed with: " << error << std::endl;
    EXPECT(error.find("rejected the connection") != std::string::npos);
    EXPECT(error.find("Number of remote connections not matching") != std::string::npos);

    EXPECT(getOrDie(server));
}


CASE("a malformed MultiBlob fails the connection, it does not crash the server") {
    // well-formed: both frames are archived
    {
        SocketPair control;
        RecordingArchiveConnection connection(control.server);
        std::string multi;
        appendFrame(multi, 1, 7, "first", 5);
        appendFrame(multi, 1, 7, "second", 6);
        connection.enqueue(remote::Message::MultiBlob, eckit::Buffer{multi.data(), multi.size()});
        connection.endOfArchive();
        EXPECT_EQUAL(connection.runArchive(), 2u);
        EXPECT_EQUAL(connection.archived.size(), 2u);
        EXPECT_EQUAL(connection.archived.at(0), std::string("first"));
        EXPECT_EQUAL(connection.archived.at(1), std::string("second"));
    }

    const auto rejected = [](const std::string& multi) {
        SocketPair control;
        RecordingArchiveConnection connection(control.server);
        connection.enqueue(remote::Message::MultiBlob, eckit::Buffer{multi.data(), multi.size()});
        EXPECT_THROWS_AS(connection.runArchive(), remote::RemoteProtocolException);
        EXPECT(connection.archived.empty());
        EXPECT(connection.aborted());
    };

    // the size of a frame overruns the message: nothing is read beyond it
    std::string overrun;
    appendFrame(overrun, 1, 7, "x", 0x10000000);
    rejected(overrun);

    // truncated header
    std::string truncated;
    appendFrame(truncated, 1, 7, "x", 1);
    rejected(truncated.substr(0, 10));

    // frame of another request
    std::string other;
    appendFrame(other, 2, 7, "x", 1);
    rejected(other);
}

CASE("a Flush on the data connection is a protocol error, it does not stop the data listener") {
    SocketPair control;
    RecordingArchiveConnection connection(control.server);
    EXPECT(connection.handleData(remote::Message::Flush, 1, 2) == remote::Handled::Replied);

    // the request gets the error
    remote::MessageHeader hdr{};
    EXPECT(control.client.read(&hdr, sizeof(hdr)) == static_cast<long>(sizeof(hdr)));
    EXPECT(hdr.message == remote::Message::Error);
    EXPECT_EQUAL(hdr.requestID, 2u);
}

CASE("an oversized or incompatible message is rejected before anything is allocated") {
    {
        SocketPair control;
        TestConnection connection(control, nullptr);
        connection.limitPayload(1024);

        const remote::MessageHeader hdr{remote::Message::Startup, true, 0, 0, 1u << 30};
        control.client.write(&hdr, sizeof(hdr));

        remote::MessageHeader received{};
        std::string error;
        try {
            connection.readControl(received);
        }
        catch (const remote::RemoteProtocolException& e) {
            error = e.what();
        }
        EXPECT(error.find("exceeds the limit of 1024 bytes") != std::string::npos);
        EXPECT(!connection.valid());
    }
    {
        SocketPair control;
        TestConnection connection(control, nullptr);

        remote::MessageHeader hdr{remote::Message::Startup, true, 0, 0, 0};
        hdr.version = remote::MessageHeader::currentVersion - 1;
        control.client.write(&hdr, sizeof(hdr));

        remote::MessageHeader received{};
        std::string error;
        try {
            connection.readControl(received);
        }
        catch (const remote::RemoteProtocolException& e) {
            error = e.what();
        }
        EXPECT(error.find("unsupported message version") != std::string::npos);
    }
}

CASE("destroying a read handle whose request is still queued neither throws nor keeps its budget") {
    eckit::net::TCPServer listener;
    listener.socket();
    const Config config = catalogueClientConfig(listener.localPort());
    ReadServerState state;
    state.accepted = 100;

    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        ReadServerConnection connection(socket, config, state);
        connection.handle();
        return true;
    });

    {
        ReadLimit limit(100);  // one field at a time
        remote::RemoteStore store(eckit::URI("fdb://127.0.0.1:" + std::to_string(listener.localPort())), config);

        auto active = readField(store, 100);
        auto queued = readField(store, 100);
        EXPECT_EQUAL(state.reads.load(), 1u);

        queued.reset();  // abandoned before being sent
        active.reset();  // abandoned while being read: frees the budget, and the abandoned one must not be sent
        EXPECT_EQUAL(state.reads.load(), 1u);

        auto next = readField(store, 100);
        EXPECT_EQUAL(state.reads.load(), 2u);
    }

    EXPECT(getOrDie(server));
}

CASE("a read that cannot be sent fails its own handle, not the one that freed the budget") {
    eckit::net::TCPServer listener;
    listener.socket();
    const Config config = catalogueClientConfig(listener.localPort());
    ReadServerState state;
    state.accepted = 1;  // the server rejects all the reads but the first

    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        ReadServerConnection connection(socket, config, state);
        connection.handle();
        return true;
    });

    {
        ReadLimit limit(100);  // one field at a time
        remote::RemoteStore store(eckit::URI("fdb://127.0.0.1:" + std::to_string(listener.localPort())), config);

        auto first = readField(store, 100);
        auto second = readField(store, 100);  // queued

        // destroying the first one sends the second one, which the server rejects: that is not the business of the
        // destructor (it must not throw), but of the reader of the second one
        first.reset();

        const auto readError = [](eckit::DataHandle& handle) {
            auto reader = std::async(std::launch::async, [&handle] {
                char buffer[100];
                try {
                    handle.openForRead();
                    handle.read(buffer, sizeof(buffer));
                    return std::string("read() returned");
                }
                catch (const std::exception& e) {
                    return std::string(e.what());
                }
            });
            return getOrDie(reader);
        };

        const auto error = readError(*second);
        eckit::Log::info() << "read of the rejected request failed with: " << error << std::endl;
        EXPECT(error.find("Unable to handle message") != std::string::npos);

        // and its budget is released: the next read is sent (and rejected) rather than queued forever
        auto third = readField(store, 100);
        EXPECT_EQUAL(state.reads.load(), 3u);
        EXPECT(readError(*third).find("Unable to handle message") != std::string::npos);
    }

    EXPECT(getOrDie(server));
}

CASE("a blocking request issued while the connection is failing is rejected, it does not wait forever") {
    eckit::net::TCPServer listener;
    listener.socket();

    // single connection: the control listener is the only reader, nothing else notices the failure meanwhile
    Config config;
    config.set("supportedConnections", std::vector<int>{1});

    std::promise<FailingOnDemandConnection*> started;
    std::promise<void> finished;
    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        FailingOnDemandConnection connection(socket, config);
        started.set_value(&connection);
        connection.handle();
        finished.get_future().wait();
        return true;
    });

    {
        const eckit::net::Endpoint endpoint("127.0.0.1", listener.localPort());
        BlockingCloseClient blocking(config, endpoint);
        TestClient other(config, endpoint);

        auto startedFuture = started.get_future();
        getOrDie(startedFuture)->sendConnectionError("synthetic connection-level failure");

        // the pending requests have been failed, the clients are being told: the connection is not invalid yet
        auto entered = blocking.entered.get_future();
        getOrDie(entered);

        auto request = std::async(std::launch::async, [&other] {
            try {
                other.controlWriteCheckResponse(remote::Message::Exists, other.generateRequestID(), false);
                return std::string("request succeeded");
            }
            catch (const std::exception& e) {
                return std::string(e.what());
            }
        });
        const auto error = getOrDie(request);
        eckit::Log::info() << "request failed with: " << error << std::endl;
        EXPECT(error.find("synthetic connection-level failure") != std::string::npos);

        blocking.release.set_value();
    }

    finished.set_value();
    EXPECT(getOrDie(server));
}


CASE("request queues: a request is unregistered on every path, and a cancelled one never blocks") {
    using Queues = remote::RequestQueues<int>;
    Queues queues;
    const auto reason = std::make_exception_ptr(eckit::SeriousBug("synthetic"));

    // not sent: unregistered again
    { auto registration = queues.add(1, 2); }
    EXPECT_EQUAL(queues.size(), 0u);

    // sent, then completed: the last message is queued, then it is unregistered
    auto sent = queues.add(1, 2).release();
    EXPECT_THROWS_AS(queues.add(1, 2), eckit::SeriousBug);  // the requestID is in use
    EXPECT(queues.enqueue(1, 10));
    EXPECT(queues.complete(1, [](Queues::Queue& q) { q.emplace(11); }));
    EXPECT_EQUAL(queues.size(), 0u);
    int value = 0;
    sent->pop(value);
    EXPECT_EQUAL(value, 10);
    sent->pop(value);
    EXPECT_EQUAL(value, 11);
    EXPECT(!queues.enqueue(1, 12));  // unknown request

    // cancelled by its consumer: the rest of the stream is discarded without blocking, even beyond the capacity, and it
    // stays registered until the server ends it
    auto abandoned = queues.add(2, 2).release();
    Queues::cancel(*abandoned, reason);
    for (int i = 0; i < 10; ++i) {
        EXPECT(queues.enqueue(2, int{i}));
    }
    EXPECT_EQUAL(queues.size(), 1u);
    EXPECT(queues.complete(2, [](Queues::Queue& q) { q.emplace(-1); }));
    EXPECT_EQUAL(queues.size(), 0u);

    // failed, one or all: the consumers get the reason, nothing stays registered (requestIDs restart on a new
    // connection)
    auto failedOne = queues.add(3, 2).release();
    auto failedAll = queues.add(4, 2).release();
    EXPECT(queues.fail(3, reason));
    queues.failAll(reason);
    EXPECT_EQUAL(queues.size(), 0u);
    EXPECT_THROWS_AS(failedOne->pop(value), eckit::SeriousBug);
    EXPECT_THROWS_AS(failedAll->pop(value), eckit::SeriousBug);
    auto reused = queues.add(4, 2);  // a new connection reuses the requestID
}


CASE("a server that never answers the handshake neither blocks the other connections nor its caller forever") {
    ScopedEnv timeout("FDB_STARTUP_TIMEOUT", "3");

    // accepts the connection, and never answers
    eckit::net::TCPServer stalled;
    stalled.socket();
    std::promise<void> release;
    auto holder = std::async(std::launch::async, [&] {
        auto& socket = stalled.accept();
        eckit::net::TCPSocket held(socket);
        release.get_future().wait();
    });
    // released also if the test fails early: the holder is joined when it is destroyed
    struct Release {
        std::promise<void>& release;
        bool done = false;
        void now() {
            if (!done) {
                done = true;
                release.set_value();
            }
        }
        ~Release() { now(); }
    } releaser{release};

    Config config;
    config.set("supportedConnections", std::vector<int>{2});

    // how long the caller of the stalled server waits: bounded by the timeout, whatever else happens meanwhile
    std::chrono::steady_clock::duration stalledWait{};
    auto stalledClient = std::async(std::launch::async, [&] {
        const auto start = std::chrono::steady_clock::now();
        std::string result;
        try {
            TestClient client(config, eckit::net::Endpoint("127.0.0.1", stalled.localPort()));
            result = "connected";
        }
        catch (const std::exception& e) {
            result = e.what();
        }
        stalledWait = std::chrono::steady_clock::now() - start;
        return result;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(300));  // it is waiting for the handshake

    // meanwhile, connecting to another server is not blocked by it
    const auto healthyStart = std::chrono::steady_clock::now();
    {
        MultiServer healthy(config, 1);
        TestClient client(config, healthy.endpoint());
        EXPECT(stalledClient.wait_for(std::chrono::seconds(0)) == std::future_status::timeout);
        eckit::Log::info() << "connected to the healthy server in "
                           << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() -
                                                                                    healthyStart)
                                  .count()
                           << " ms" << std::endl;
    }
    eckit::Log::info() << "healthy connection opened and closed in "
                       << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() -
                                                                                healthyStart)
                              .count()
                       << " ms" << std::endl;

    const auto error = getOrDie(stalledClient);
    eckit::Log::info() << "stalled connection failed with: " << error << std::endl;
    EXPECT(error.find("did not answer the startup message") != std::string::npos);
    eckit::Log::info() << "the caller of the stalled server waited "
                       << std::chrono::duration_cast<std::chrono::milliseconds>(stalledWait).count() << " ms"
                       << std::endl;
    EXPECT(stalledWait < std::chrono::seconds(10));  // the timeout is 3 s

    releaser.now();
    holder.get();
}

CASE("a client that never opens its data connection does not hold the server forever") {
    ScopedEnv timeout("FDB_DATA_CONNECTION_TIMEOUT", "1");

    eckit::net::TCPServer listener;
    listener.socket();
    Config config;
    config.set("supportedConnections", std::vector<int>{2});

    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        FailingOnDemandConnection connection(socket, config);
        connection.handle();
        return true;
    });

    // a client that sends its startup message, and goes no further
    eckit::net::TCPClient client;
    client.connect("127.0.0.1", listener.localPort());
    {
        eckit::Buffer payload(4096);
        eckit::MemoryStream s(payload);
        s << eckit::SessionID();
        s << eckit::net::Endpoint("127.0.0.1", listener.localPort());
        s << LibFdb5::instance().remoteProtocolVersion().used();
        s << remote::RemoteConfiguration(config);
        const remote::MessageHeader hdr{remote::Message::Startup, true, 0, 0, static_cast<uint32_t>(s.position())};
        client.write(&hdr, sizeof(hdr));
        client.write(payload.data(), s.position());
        client.write(remote::MessageHeader::EndMarker.data(), remote::MessageHeader::markerBytes);
    }

    // the server gives up waiting for the data connection
    EXPECT(getOrDie(server));
}

CASE("connections are shared by endpoint and default endpoint, and a retired one is never reused") {
    Config config;
    config.set("supportedConnections", std::vector<int>{2});
    MultiServer server(config, 3);

    {
        TestClient alias(config, server.endpoint(), "alias:1234");
        TestClient canonical(config, server.endpoint(), "canonical:1234");
        TestClient sameAlias(config, server.endpoint(), "alias:1234");

        // the default endpoint builds the field locations: never shared between different ones
        EXPECT(alias.connectionAddress() != canonical.connectionAddress());
        EXPECT(alias.connectionAddress() == sameAlias.connectionAddress());
    }

    // all their clients gone, the connections are torn down: a new client gets a new connection, which works (the
    // request is answered, here rejected by the test server)
    TestClient later(config, server.endpoint(), "alias:1234");
    std::string answer;
    try {
        later.controlWriteCheckResponse(remote::Message::Exists, later.generateRequestID(), false);
    }
    catch (const std::exception& e) {
        answer = e.what();
    }
    EXPECT(answer.find("Unable to handle message") != std::string::npos);
}


CASE("an Exit from the server ends the streamed requests in single-connection mode, and the connection") {
    eckit::net::TCPServer listener;
    listener.socket();

    Config config;
    config.set("supportedConnections", std::vector<int>{1});  // no data listener on the client

    std::promise<FailingOnDemandConnection*> started;
    std::promise<void> ended;
    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        FailingOnDemandConnection connection(socket, config);
        started.set_value(&connection);
        connection.handle();
        ended.get_future().wait();  // end() is still using the connection
        return true;
    });

    {
        remote::RemoteStore store(eckit::URI("fdb://127.0.0.1:" + std::to_string(listener.localPort())), config);
        auto startedFuture = started.get_future();
        FailingOnDemandConnection* connection = getOrDie(startedFuture);
        waitForServer(store);

        auto reader = startStoreReader(remote::RemoteStoreTestHarness::registerQueue(store).second);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        connection->end();
        ended.set_value();

        const auto error = getOrDie(reader);
        eckit::Log::info() << "read ended with: " << error << std::endl;
        EXPECT(error.find("closed") != std::string::npos);
        EXPECT(!store.connectionUsable());
    }

    EXPECT(getOrDie(server));
}

CASE("the sockets of a failed connection are released, even if its client lives on") {
    eckit::net::TCPServer listener;
    listener.socket();
    const Config config = catalogueClientConfig(listener.localPort());  // dual connection: 2 sockets

    const size_t before = openDescriptors();

    std::promise<FailingOnDemandConnection*> started;
    std::promise<void> failed;
    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        FailingOnDemandConnection connection(socket, config);
        started.set_value(&connection);
        connection.handle();
        failed.get_future().wait();
        return true;
    });

    // e.g. a read store retired by RemoteStore::get(): it is kept, and so is its connection
    remote::RemoteStore store(eckit::URI("fdb://127.0.0.1:" + std::to_string(listener.localPort())), config);
    auto startedFuture = started.get_future();
    getOrDie(startedFuture)->fail("synthetic connection failure");
    failed.set_value();
    EXPECT(getOrDie(server));  // the server side is gone: only the descriptors of the client could remain

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    size_t after = openDescriptors();
    while (after > before && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        after = openDescriptors();
    }
    eckit::Log::info() << "open descriptors: " << before << " before, " << after << " after the failure" << std::endl;
    EXPECT(!store.connectionUsable());
    EXPECT(after <= before);
}


CASE("a failure of the data connection alone ends the connection, and releases its sockets") {
    eckit::net::TCPServer listener;
    listener.socket();
    const Config config = catalogueClientConfig(listener.localPort());  // dual connection

    std::promise<FailingOnDemandConnection*> started;
    std::promise<void> failed;
    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        FailingOnDemandConnection connection(socket, config);
        started.set_value(&connection);
        connection.handle();
        failed.get_future().wait();
        return true;
    });

    auto store = std::make_unique<remote::RemoteStore>(
        eckit::URI("fdb://127.0.0.1:" + std::to_string(listener.localPort())), config);
    auto startedFuture = started.get_future();
    FailingOnDemandConnection* connection = getOrDie(startedFuture);
    waitForServer(*store);
    const size_t connected = openDescriptors();

    // only the data connection fails: the server neither sends an error nor closes the control connection
    connection->failDataOnly();

    // the client ends the connection on both channels, after waiting (bounded) for an error of the server, and
    // releases its two sockets (the server keeps its own ones meanwhile)
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    size_t after = openDescriptors();
    while (after > connected - 2 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        after = openDescriptors();
    }
    eckit::Log::info() << "open descriptors: " << connected << " connected, " << after << " after the failure"
                       << std::endl;
    const bool released = after <= connected - 2;
    if (!released) {
        // its destruction would wait forever for the listener of the healthy control socket: report, don't hang
        static_cast<void>(store.release());
        connection->fail("end of the test");  // and the server would wait for the client
    }
    if (!released) {
        failed.set_value();
    }
    EXPECT(released);
    EXPECT(!store->connectionUsable());

    // and the connection can be destroyed: nothing is left waiting on the healthy control socket
    auto destroyed = std::async(std::launch::async, [&store] {
        store.reset();
        return true;
    });
    EXPECT(getOrDie(destroyed));

    failed.set_value();  // the server connection may now be destroyed
    EXPECT(getOrDie(server));
}


CASE("a fatal error while another writer is blocked ends the connection, it does not wait for the writer") {
    eckit::net::TCPServer listener;
    listener.socket();
    Config config;
    config.set("supportedConnections", std::vector<int>{2});
    BlockingDataState state;

    std::promise<BlockingDataConnection*> started;
    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        BlockingDataConnection connection(socket, config, state);
        started.set_value(&connection);
        connection.handle();
        return true;
    });

    // the server stops reading the data connection, released at the end of the test (also if it fails)
    struct Release {
        BlockingDataState& state;
        bool done = false;
        void now() {
            if (!done) {
                done = true;
                state.release.set_value();
            }
        }
        ~Release() { now(); }
    } release{state};

    {
        TestClient client(config, eckit::net::Endpoint("127.0.0.1", listener.localPort()));
        auto startedFuture = started.get_future();
        BlockingDataConnection* connection = getOrDie(startedFuture);

        client.controlWriteCheckResponse(remote::Message::Store, client.generateRequestID(), true);
        const char byte = 'x';
        client.dataWrite(remote::Message::Blob, 100, {{1, &byte}});  // the server blocks on it, and stops reading
        auto blocked = state.blocked.get_future();
        getOrDie(blocked);

        // the data writer of the client blocks on the full socket, holding its mutex
        std::vector<char> payload(16 * 1024 * 1024, 'x');
        client.dataWrite(remote::Message::Blob, 101, {{payload.size(), payload.data()}});
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        // while the connection is still valid, a reply nobody expects: a fatal error for the client
        connection->write(remote::Message::Received, true, client.id(), 4242);

        // the connection is ended without waiting for the blocked writer, which is released: the queued writes fail
        auto flushed = std::async(std::launch::async, [&client] {
            try {
                client.flushData();
                return std::string("flushed");
            }
            catch (const std::exception& e) {
                return std::string(e.what());
            }
        });
        const auto result = getOrDie(flushed);
        eckit::Log::info() << "queued writes ended with: " << result << std::endl;
        EXPECT(result != "flushed");
        EXPECT(!client.connectionUsable());
    }

    release.now();
    EXPECT(getOrDie(server));
}


CASE("destroying a handler whose workers are still running terminates the process, rather than corrupting it") {
    // in a child process: the expected outcome is its termination
    const pid_t child = ::fork();
    ASSERT(child >= 0);
    if (child == 0) {
        {
            SocketPair control;
            MisbehavingConnection connection(control.server);
        }  // the base destructor finds the archive worker still running
        std::_Exit(0);
    }

    int status = 0;
    ASSERT(::waitpid(child, &status, 0) == child);
    EXPECT(WIFSIGNALED(status));
    if (WIFSIGNALED(status)) {
        EXPECT_EQUAL(WTERMSIG(status), SIGABRT);
    }
}


CASE("a startup answer that stops (or slows down) in the middle does not block its caller beyond the deadline") {
    ScopedEnv timeout("FDB_STARTUP_TIMEOUT", "3");

    Config config;
    config.set("supportedConnections", std::vector<int>{2});

    // a server that answers the startup message with the given bytes, one every delay, then stalls
    const auto attempt = [&config](const std::string& answer, std::chrono::milliseconds delay) {
        eckit::net::TCPServer server;
        server.socket();
        std::promise<void> release;
        std::shared_future<void> released = release.get_future().share();
        auto answering = std::async(std::launch::async, [&] {
            eckit::net::TCPSocket socket(server.accept());
            if (delay.count() == 0) {
                socket.write(answer.data(), answer.size());
            }
            else {
                for (char c : answer) {
                    socket.write(&c, 1);
                    if (released.wait_for(delay) == std::future_status::ready) {
                        return;
                    }
                }
            }
            released.wait();
        });

        const auto start = std::chrono::steady_clock::now();
        std::string error;
        try {
            TestClient client(config, eckit::net::Endpoint("127.0.0.1", server.localPort()));
            error = "connected";
        }
        catch (const std::exception& e) {
            error = e.what();
        }
        const auto waited = std::chrono::steady_clock::now() - start;
        release.set_value();
        answering.get();

        eckit::Log::info() << "waited " << std::chrono::duration_cast<std::chrono::milliseconds>(waited).count()
                           << " ms, failed with: " << error << std::endl;
        EXPECT(error.find("did not answer the startup message") != std::string::npos);
        EXPECT(waited < std::chrono::seconds(10));
    };

    const remote::MessageHeader header{remote::Message::Startup, true, 0, 0, 100};
    const std::string headerBytes(reinterpret_cast<const char*>(&header), sizeof(header));

    // half a header
    attempt(headerBytes.substr(0, sizeof(header) / 2), std::chrono::milliseconds(0));
    // a header, and part of its payload
    attempt(headerBytes + std::string(10, 'x'), std::chrono::milliseconds(0));
    // a header, a byte at a time: each read gets something, the whole answer never comes in time
    attempt(headerBytes, std::chrono::milliseconds(500));
}


CASE("failover: a rejection by every endpoint is reported as such, an unreachable one keeps the error retryable") {
    Config serverConfig;
    serverConfig.set("supportedConnections", std::vector<int>{2});
    Config clientConfig;
    clientConfig.set("supportedConnections", std::vector<int>{1});  // rejected by the servers

    // what the client gets: its message, and whether it is worth retrying
    const auto failure = [&clientConfig](const remote::Client::EndpointList& endpoints) {
        try {
            TestClient client(clientConfig, endpoints);
        }
        catch (const eckit::Exception& e) {
            return std::make_pair(std::string(e.what()), e.retryOnClient());
        }
        return std::make_pair(std::string("connected"), false);
    };

    const auto unreachableEndpoint = [] {
        eckit::net::TCPServer closed;
        closed.socket();
        return eckit::net::Endpoint("127.0.0.1", closed.localPort());  // closed when returning
    };

    {
        MultiServer first(serverConfig, 1);
        MultiServer second(serverConfig, 1);
        const auto [error, retryable] = failure({{first.endpoint(), ""}, {second.endpoint(), ""}});
        eckit::Log::info() << "all rejected: " << error << std::endl;
        EXPECT(error.find("Number of remote connections not matching") != std::string::npos);
        EXPECT(!retryable);
    }
    {
        MultiServer rejecting(serverConfig, 1);
        const auto [error, retryable] = failure({{rejecting.endpoint(), ""}, {unreachableEndpoint(), ""}});
        eckit::Log::info() << "one rejected, one unreachable: " << error << std::endl;
        EXPECT(error.find("Number of remote connections not matching") != std::string::npos);
        EXPECT(retryable);
    }

    // servers that accept the connection, and never answer: overloaded, they may answer later
    ScopedEnv timeout("FDB_STARTUP_TIMEOUT", "2");
    struct Stalled {
        Stalled() {
            server.socket();
            holder = std::async(std::launch::async, [this] {
                eckit::net::TCPSocket socket(server.accept());
                released.wait();
            });
        }
        ~Stalled() {
            release.set_value();
            holder.get();
        }
        eckit::net::Endpoint endpoint() { return {"127.0.0.1", server.localPort()}; }

        eckit::net::TCPServer server;
        std::promise<void> release;
        std::shared_future<void> released = release.get_future().share();
        std::future<void> holder;
    };
    {
        Stalled first;
        Stalled second;
        const auto [error, retryable] = failure({{first.endpoint(), ""}, {second.endpoint(), ""}});
        eckit::Log::info() << "all timed out: " << error << std::endl;
        EXPECT(error.find("did not answer the startup message") != std::string::npos);
        EXPECT(retryable);
    }
    {
        MultiServer rejecting(serverConfig, 1);
        Stalled stalled;
        const auto [error, retryable] = failure({{rejecting.endpoint(), ""}, {stalled.endpoint(), ""}});
        eckit::Log::info() << "one rejected, one timed out: " << error << std::endl;
        EXPECT(error.find("did not answer the startup message") != std::string::npos);
        EXPECT(error.find("Number of remote connections not matching") != std::string::npos);
        EXPECT(retryable);
    }
}


CASE("the failure of the last queued location is published before it stops counting as pending") {
    SocketPair control;
    FailingLastLocationConnection connection(control.server);
    connection.enqueue();
    EXPECT_EQUAL(connection.pending(), 1u);

    auto archive = std::async(std::launch::async, [&connection] {
        try {
            connection.runArchive();
        }
        catch (...) {
        }
    });

    // what a flush does: as soon as nothing is pending any more, the failure must be visible, otherwise it would report
    // missing locations instead of the failure
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (connection.pending() != 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    EXPECT_EQUAL(connection.pending(), 0u);
    EXPECT(connection.failureRecorded());

    getOrDie(archive);
}


CASE("an Exit on the control channel only ends the streamed requests too, after a bounded wait for the data channel") {
    eckit::net::TCPServer listener;
    listener.socket();
    const Config config = catalogueClientConfig(listener.localPort());  // dual connection

    std::promise<FailingOnDemandConnection*> started;
    std::promise<void> ended;
    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        FailingOnDemandConnection connection(socket, config);
        started.set_value(&connection);
        connection.handle();
        ended.get_future().wait();
        return true;
    });

    auto store = std::make_unique<remote::RemoteStore>(
        eckit::URI("fdb://127.0.0.1:" + std::to_string(listener.localPort())), config);
    auto startedFuture = started.get_future();
    FailingOnDemandConnection* connection = getOrDie(startedFuture);
    waitForServer(*store);

    auto reader = startStoreReader(remote::RemoteStoreTestHarness::registerQueue(*store).second);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const auto start = std::chrono::steady_clock::now();
    connection->endControlOnly();

    // the read waiting for the data channel is ended (after the bounded wait for the data channel to end)
    const auto error = getOrDie(reader);
    eckit::Log::info()
        << "read ended after "
        << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count()
        << " ms with: " << error << std::endl;
    EXPECT(error.find("closed") != std::string::npos);

    // and the connection can be destroyed: nothing is left waiting on the open data channel
    auto destroyed = std::async(std::launch::async, [&store] {
        store.reset();
        return true;
    });
    EXPECT(getOrDie(destroyed));

    ended.set_value();
    EXPECT(getOrDie(server));
}

CASE("a read store whose connection is lost is replaced, and the threads of its connection are joined") {
    eckit::net::TCPServer listener;
    listener.socket();
    const eckit::URI uri("fdb://127.0.0.1:" + std::to_string(listener.localPort()));
    Config serverConfig;  // n.b. RemoteStore::get() uses the default configuration

    std::promise<FailingOnDemandConnection*> started;
    std::promise<void> failed;
    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        FailingOnDemandConnection connection(socket, serverConfig);
        started.set_value(&connection);
        connection.handle();
        failed.get_future().wait();
        return true;
    });

    remote::RemoteStore& lost = remote::RemoteStore::get(uri);
    auto startedFuture = started.get_future();
    getOrDie(startedFuture)->fail("synthetic connection failure");
    failed.set_value();
    EXPECT(getOrDie(server));

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (lost.connectionUsable() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT(!lost.connectionUsable());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));  // its listeners stop
    EXPECT(!remote::RemoteStoreTestHarness::connectionThreadsReclaimed(lost));

    // The next read replaces the store (here the server does not answer any more: the new connection fails, which
    // does not matter). The lost store is kept, as other threads may still use it, but not the threads of its
    // connection.
    {
        ScopedEnv timeout("FDB_STARTUP_TIMEOUT", "1");
        try {
            static_cast<void>(remote::RemoteStore::get(uri));
        }
        catch (const std::exception& e) {
            eckit::Log::info() << "reconnection failed with: " << e.what() << std::endl;
        }
    }
    EXPECT(remote::RemoteStoreTestHarness::connectionThreadsReclaimed(lost));
}


CASE("a writer holding the queue while the connection ends and its threads are joined gets a connection error") {
    eckit::net::TCPServer listener;
    listener.socket();
    const Config config = catalogueClientConfig(listener.localPort());  // dual connection

    std::promise<DiscardingConnection*> started;
    std::promise<void> ended;
    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        DiscardingConnection connection(socket, config);
        started.set_value(&connection);
        connection.handle();
        ended.get_future().wait();
        return true;
    });

    TestClient client(config, eckit::net::Endpoint("127.0.0.1", listener.localPort()));
    auto startedFuture = started.get_future();
    DiscardingConnection* connection = getOrDie(startedFuture);
    client.controlWriteCheckResponse(remote::Message::Store, client.generateRequestID(), true);

    // the server connection is destroyed only at the end, also if the test fails before
    struct End {
        std::promise<void>& ended;
        bool done = false;
        void now() {
            if (!done) {
                done = true;
                ended.set_value();
            }
        }
        ~End() { now(); }
    } end{ended};

    // the writer of the connection is started, and idle
    const char byte = 'x';
    client.dataWrite(remote::Message::Blob, 100, {{1, &byte}});

    // A write that pauses once it holds the queue: the connection was valid
    WriteGate gate;
    auto writer = std::async(std::launch::async, [&client, &byte] {
        try {
            client.dataWrite(remote::Message::Blob, 101, {{1, &byte}});
            return std::string("queued");
        }
        catch (const eckit::Exception& e) {
            return std::string(e.what());
        }
    });
    // n.b. declared after the future: released before the future waits for the write, also if the test fails
    struct Release {
        WriteGate& gate;
        ~Release() { gate.release(); }
    } release{gate};
    EXPECT(gate.awaitPaused(std::chrono::seconds(30)));

    // Meanwhile the connection ends, and its threads are joined: the idle writer finds its queue closed, and releases
    // it. Only then does the write go on, and queue its request.
    connection->end();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!client.reclaimConnectionThreads() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT(client.reclaimConnectionThreads());
    gate.release();

    // it gets a connection error from the closed queue it holds, never a released queue
    const auto result = getOrDie(writer);
    eckit::Log::info() << "paused write ended with: " << result << std::endl;
    EXPECT(result.find("is no longer valid") != std::string::npos);

    end.now();
    EXPECT(getOrDie(server));
}


CASE("a startup message that cannot be decoded is reported as such to the client") {
    eckit::net::TCPServer listener;
    listener.socket();
    Config config;
    config.set("supportedConnections", std::vector<int>{2});

    auto server = std::async(std::launch::async, [&] {
        auto& socket = listener.accept();
        FailingOnDemandConnection connection(socket, config);
        connection.handle();
        return true;
    });

    // a startup message whose payload is not a startup message
    eckit::net::TCPClient client;
    client.connect("127.0.0.1", listener.localPort());
    const std::string garbage = "not a startup message";
    const remote::MessageHeader hdr{remote::Message::Startup, true, 0, 0, static_cast<uint32_t>(garbage.size())};
    client.write(&hdr, sizeof(hdr));
    client.write(garbage.data(), garbage.size());
    client.write(remote::MessageHeader::EndMarker.data(), remote::MessageHeader::markerBytes);

    // the server answers with the reason, and closes the connection
    remote::MessageHeader reply{};
    EXPECT(client.read(&reply, sizeof(reply)) == static_cast<long>(sizeof(reply)));
    EXPECT(reply.message == remote::Message::Error);
    std::string reason(reply.payloadSize, ' ');
    EXPECT(client.read(reason.data(), reason.size()) == static_cast<long>(reason.size()));
    eckit::Log::info() << "server answered: " << reason << std::endl;
    EXPECT(reason.find("invalid Startup message") != std::string::npos);

    EXPECT(getOrDie(server));
}

}  // namespace fdb5::test

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}

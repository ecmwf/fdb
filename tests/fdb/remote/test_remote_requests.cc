// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// The lifetime of the requests whose answer is streamed by the server: abandoning one must not affect the others
// sharing the connection.
//
// Run by test_server.sh.in, against a catalogue and a store server.

#include "RemoteStoreTestHarness.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/database/Key.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/remote/client/Client.h"
#include "fdb5/remote/client/RemoteCatalogue.h"
#include "fdb5/remote/client/RemoteStore.h"
#include "fdb5/toc/TocFieldLocation.h"

#include "metkit/mars/MarsRequest.h"

#include "eckit/io/Buffer.h"
#include "eckit/io/DataHandle.h"
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"

#include <sys/types.h>

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace eckit::testing;

namespace fdb5::test {

namespace {

const std::string data = "It's gonna be a bright, sunshiny day!";

// More fields than the queue of a list request on the client holds (100 messages)
constexpr size_t numFields = 250;

void archiveFields(const std::string& date) {
    FDB fdb{};
    for (size_t step = 1; step <= numFields; ++step) {
        Key key;
        key.set("class", "od");
        key.set("expver", "xxxx");
        key.set("stream", "oper");
        key.set("time", "0000");
        key.set("domain", "g");
        key.set("levtype", "sfc");
        key.set("param", "167");
        key.set("date", date);
        key.set("type", "fc");
        key.set("step", std::to_string(step));
        fdb.archive(key, data.data(), data.size());
    }
    fdb.flush();
}

FDBToolRequest listRequest(const std::string& date) {
    return FDBToolRequest::requestsFromString("class=od,expver=xxxx,date=" + date)[0];
}

// A request that never completes fails the test instead of hanging it
template <typename T>
T getOrDie(std::future<T>& future, const char* what) {
    if (future.wait_for(std::chrono::seconds(60)) != std::future_status::ready) {
        std::cerr << what << " still blocked" << std::endl;
        std::_Exit(1);
    }
    return future.get();
}


void killServer(const char* pgidVariable) {
    const char* pgid = std::getenv(pgidVariable);
    ASSERT(pgid != nullptr);
    const long pgidValue = std::atol(pgid);
    ASSERT(pgidValue > 0);
    ASSERT(::kill(-pgidValue, SIGKILL) == 0);
}

void restartServer(const char* scriptVariable) {
    const char* script = std::getenv(scriptVariable);
    ASSERT(script != nullptr);
    ASSERT(std::system(script) == 0);
}


// A client of the store server that archives one field, and goes away without waiting: its Stop can overtake its field
class AbandoningStoreClient : public remote::Client {
public:

    explicit AbandoningStoreClient(const Config& config) :
        Client(config, eckit::net::Endpoint("localhost", storePort()), ""), config_(config) {}
    ~AbandoningStoreClient() override { deregister(); }

    void archiveLargeField(const Key& dbKey, const Key& idxKey) {
        const uint32_t id = generateRequestID();
        controlWriteCheckResponse(remote::Message::Store, id, true);

        eckit::Buffer keys(4096);
        eckit::MemoryStream s(keys);
        s << dbKey;
        s << idxKey;
        static const std::vector<char> data(64 * 1024 * 1024, 'x');  // takes a while to reach the server
        dataWrite(remote::Message::Blob, id, {{s.position(), keys.data()}, {data.size(), data.data()}});
    }

    const eckit::Configuration& clientConfig() const override { return config_; }
    bool handle(remote::Message, uint32_t) override { return true; }
    bool handle(remote::Message, uint32_t, eckit::Buffer&&) override { return true; }

private:

    // the port of the store server, from its configuration (next to the configuration of the client)
    static int storePort() {
        const char* clientConfig = std::getenv("FDB5_CONFIG_FILE");
        ASSERT(clientConfig != nullptr);
        const eckit::PathName storeConfig = eckit::PathName(clientConfig).dirName() / "store.yaml";
        return eckit::YAMLConfiguration(storeConfig).getInt("serverPort");
    }

    Config config_;
};

// True if a database directory of the store root is for the given date
bool storeHasDatabase(const std::string& date) {
    const char* root = std::getenv("FDB_REMOTE_STORE_ROOT");
    ASSERT(root != nullptr);
    std::vector<eckit::PathName> files;
    std::vector<eckit::PathName> dirs;
    eckit::PathName(root).children(files, dirs);
    for (const auto& dir : dirs) {
        if (dir.baseName().asString().find(date) != std::string::npos) {
            return true;
        }
    }
    return false;
}


// A catalogue client that flushes more field locations than it archived (a protocol violation)
class ShortFlushCatalogueClient : public remote::Client {
public:

    explicit ShortFlushCatalogueClient(const Config& config) :
        Client(config, eckit::net::Endpoint(clientSetting("host"), std::stoi(clientSetting("port"))), ""),
        config_(config) {}
    ~ShortFlushCatalogueClient() override { deregister(); }

    void openDatabase(const Key& dbKey) {
        eckit::Buffer buffer(4096);
        eckit::MemoryStream s(buffer);
        s << dbKey;
        static_cast<void>(controlWriteReadResponse(remote::Message::Schema, generateRequestID(), buffer, s.position()));
    }

    void archive(const Key& idxKey, const Key& datumKey, const FieldLocation& location) {
        const uint32_t id = generateRequestID();
        if (!archiving_) {
            controlWriteCheckResponse(remote::Message::Archive, id, true);
            archiving_ = true;
        }
        eckit::Buffer buffer(8192);
        eckit::MemoryStream s(buffer);
        s << idxKey;
        s << datumKey;
        s << location;
        dataWrite(remote::Message::Blob, id, {{s.position(), buffer.data()}});
    }

    void flush(size_t count) {
        eckit::Buffer buffer(1024);
        eckit::MemoryStream s(buffer);
        s << count;
        controlWriteCheckResponse(remote::Message::Flush, generateRequestID(), false, buffer, s.position());
    }

    const eckit::Configuration& clientConfig() const override { return config_; }
    bool handle(remote::Message, uint32_t) override { return true; }
    bool handle(remote::Message, uint32_t, eckit::Buffer&&) override { return true; }

private:

    // the endpoint of the catalogue server, from the configuration of the client
    static std::string clientSetting(const std::string& name) {
        const char* clientConfig = std::getenv("FDB5_CONFIG_FILE");
        ASSERT(clientConfig != nullptr);
        const eckit::YAMLConfiguration config{eckit::PathName(clientConfig)};
        return name == "port" ? std::to_string(config.getInt(name)) : config.getString(name);
    }

    Config config_;
    bool archiving_ = false;
};

}  // namespace

//-----------------------------------------------------------------------------

CASE("Remote FDB: an abandoned list does not block the other requests of the connection") {
    const std::string date = "20600101";
    archiveFields(date);

    FDB fdb{};
    {
        // read one element, and abandon the rest of the stream
        auto it = fdb.list(listRequest(date));
        ListElement elem;
        EXPECT(it.next(elem));
    }

    // the same connection keeps serving: the rest of the abandoned stream is discarded
    auto counted = std::async(std::launch::async, [&fdb, &date] {
        auto it = fdb.list(listRequest(date));
        size_t count = 0;
        ListElement elem;
        while (it.next(elem)) {
            ++count;
        }
        return count;
    });
    EXPECT_EQUAL(getOrDie(counted, "list after an abandoned list"), numFields);
}

CASE("Remote FDB: a catalogue client that goes away without flushing does not break its connection") {
    const std::string date = "20600102";
    archiveFields(date);  // creates the database

    const Config config = Config{}.expandConfig();
    FDB fdb{};  // shares the connection to the catalogue server

    {
        Key dbKey;
        dbKey.set("class", "od");
        dbKey.set("expver", "xxxx");
        dbKey.set("stream", "oper");
        dbKey.set("time", "0000");
        dbKey.set("domain", "g");
        dbKey.set("date", date);

        remote::RemoteCatalogue catalogue(dbKey, config);
        catalogue.schema();  // the server opens the database for this client

        Key idxKey;
        idxKey.set("type", "fc");
        idxKey.set("levtype", "sfc");
        for (size_t i = 0; i < 2000; ++i) {
            Key datumKey;
            datumKey.set("step", std::to_string(10000 + i));
            datumKey.set("param", "167");
            catalogue.archive(idxKey, datumKey,
                              std::make_shared<TocFieldLocation>(eckit::PathName("/dev/null"), i, 1, Key{}));
        }
        // destroyed without a flush: its Stop may reach the server before the locations still queued
    }

    // the connection keeps serving the other client
    auto counted = std::async(std::launch::async, [&fdb, &date] {
        auto it = fdb.list(listRequest(date));
        size_t count = 0;
        ListElement elem;
        while (it.next(elem)) {
            ++count;
        }
        return count;
    });
    EXPECT_EQUAL(getOrDie(counted, "list after a catalogue client went away"), numFields);
}

CASE("Remote store: a field of a client that went away before it was archived is discarded") {
    const std::string date = "20600103";

    {
        AbandoningStoreClient client(Config{});
        Key dbKey;
        dbKey.set("class", "od");
        dbKey.set("expver", "xxxx");
        dbKey.set("stream", "oper");
        dbKey.set("date", date);
        dbKey.set("time", "0000");
        dbKey.set("domain", "g");
        Key idxKey;
        idxKey.set("type", "fc");
        idxKey.set("levtype", "sfc");
        client.archiveLargeField(dbKey, idxKey);
    }  // its Stop reaches the server before the end of its field

    // the server processes the field meanwhile: it must not create a store (a database) for the removed client
    std::this_thread::sleep_for(std::chrono::seconds(3));
    EXPECT(!storeHasDatabase(date));
}

CASE("Remote catalogue: a flush of more locations than can ever arrive fails, it does not wait forever") {
    const std::string date = "20600104";

    Config config;
    config.set("supportedConnections", std::vector<int>{1});  // single connection: no location arrives during a flush

    ShortFlushCatalogueClient client(config);
    Key dbKey;
    dbKey.set("class", "od");
    dbKey.set("expver", "xxxx");
    dbKey.set("stream", "oper");
    dbKey.set("date", date);
    dbKey.set("time", "0000");
    dbKey.set("domain", "g");
    client.openDatabase(dbKey);

    Key idxKey;
    idxKey.set("type", "fc");
    idxKey.set("levtype", "sfc");
    for (size_t step = 1; step <= 2; ++step) {
        Key datumKey;
        datumKey.set("step", std::to_string(step));
        datumKey.set("param", "167");
        client.archive(idxKey, datumKey, TocFieldLocation(eckit::PathName("/dev/null"), step, 1, Key{}));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));  // the locations are sent before the flush

    auto flushed = std::async(std::launch::async, [&client] {
        try {
            client.flush(3);  // one more than archived
            return std::string("flushed");
        }
        catch (const std::exception& e) {
            return std::string(e.what());
        }
    });
    const auto error = getOrDie(flushed, "flush of more locations than archived");
    eckit::Log::info() << "flush failed with: " << error << std::endl;
    EXPECT(error.find("Flush of 3 field locations, but only 2 were received") != std::string::npos);
}

CASE("Remote store: reads reconnect once the store is restarted, rather than failing forever") {

    remote::RemoteStore& before = storeForOneField();
    const eckit::URI uri = before.uri();

    // n.b. the servers can be restarted once per test run: this is the last case
    killServer("FDB_REMOTE_STORE_PGID");
    restartServer("FDB_REMOTE_STORE_RESTART");

    // the client notices the lost connection
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (before.connectionUsable() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT(!before.connectionUsable());

    // the cached read store is replaced by one with a new connection
    remote::RemoteStore& after = remote::RemoteStore::get(uri);
    EXPECT(&after != &before);
    EXPECT(after.connectionUsable());

    // and the field archived before the restart can be read
    FDB fdb{};
    metkit::mars::MarsRequest request("retrieve");
    for (const auto& [k, v] : std::map<std::string, std::string>{
             {"class", "od"}, {"expver", "xxxx"}, {"stream", "oper"}, {"time", "0000"}, {"domain", "g"},
             {"levtype", "sfc"}, {"param", "167"}, {"date", "20500101"}, {"type", "fc"}, {"step", "1"}}) {
        request.setValue(k, v);
    }
    std::unique_ptr<eckit::DataHandle> dh(fdb.retrieve(request));
    eckit::AutoClose closer(*dh);
    dh->openForRead();
    const std::string expected("test data for remote store tests");
    eckit::Buffer buffer(expected.size());
    EXPECT_EQUAL(dh->read(buffer, buffer.size()), static_cast<long>(expected.size()));
    EXPECT_EQUAL(std::string(static_cast<const char*>(buffer), buffer.size()), expected);
}

}  // namespace fdb5::test

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}

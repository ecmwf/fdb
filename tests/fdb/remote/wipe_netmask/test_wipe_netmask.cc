// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// Verifies that the server-side `networks` configuration restricts wipe to the clients of a given netmask (FDB-663).
///
/// The environment (two catalogue/store server pairs) is configured by an external script. See wipe_netmask.sh.in.
///  - "allowed" servers grant wipe to the netmask containing the test client (127.0.0.0/8) only
///  - "denied" servers grant wipe only to a netmask not containing the test client (10.0.0.0/8)

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/io/Buffer.h"
#include "eckit/io/DataHandle.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

using namespace eckit::testing;

namespace fdb5::test {

//-----------------------------------------------------------------------------

namespace {

constexpr auto* k_wipe_request = "class=od,expver=xxxx";
const std::string k_data = "wipe-netmask-test-data";

// The clients only differ by the catalogue server they connect to: its port is given by the script
Config client_config(const char* portVariable) {
    const char* port = std::getenv(portVariable);
    ASSERT_MSG(port, std::string("Environment variable not set: ") + portVariable);

    Config config;
    config.set("type", "remote");
    config.set("store", "remote");
    config.set("engine", "remote");
    config.set("host", "localhost");
    config.set("port", std::stoi(port));
    return config;
}

std::vector<Key> fields() {
    std::vector<Key> keys;
    for (const auto* date : {"20101010", "20111213"}) {
        for (const auto* step : {"1", "2"}) {
            keys.push_back(Key{{"class", "od"},
                               {"expver", "xxxx"},
                               {"stream", "oper"},
                               {"date", date},
                               {"time", "0000"},
                               {"domain", "g"},
                               {"type", "fc"},
                               {"levtype", "sfc"},
                               {"step", step},
                               {"param", "167"}});
        }
    }
    return keys;
}

void archive_data(FDB& fdb) {
    for (const auto& key : fields()) {
        fdb.archive(key, k_data.data(), k_data.size());
    }
    fdb.flush();
}

size_t count_fields(FDB& fdb) {
    size_t count = 0;
    auto it = fdb.list(FDBToolRequest{{}, true, {}}, true);
    ListElement elem;
    while (it.next(elem)) {
        ++count;
    }
    return count;
}

// The data of a field, as read from the store
std::string retrieve(FDB& fdb, const Key& key) {
    std::unique_ptr<eckit::DataHandle> dh(fdb.retrieve(key.request("retrieve")));
    eckit::AutoClose closer(*dh);
    dh->openForRead();
    eckit::Buffer buffer(k_data.size());
    const long length = dh->read(buffer, buffer.size());
    return {static_cast<const char*>(buffer), static_cast<size_t>(std::max(length, 0L))};
}

FDBToolRequest wipe_request() {
    return FDBToolRequest::requestsFromString(k_wipe_request)[0];
}

}  // namespace

//-----------------------------------------------------------------------------

CASE("Remote wipe: client within the netmask granted wipe can wipe") {
    FDB fdb{client_config("FDB_TEST_CATALOGUE_ALLOWED_PORT")};

    archive_data(fdb);
    EXPECT_EQUAL(count_fields(fdb), fields().size());

    auto it = fdb.wipe(wipe_request(), true);
    size_t wiped = 0;
    WipeElement elem;
    while (it.next(elem)) {
        ++wiped;
    }
    EXPECT(wiped > 0);
    EXPECT_EQUAL(count_fields(fdb), 0);
}

CASE("Remote wipe: client outside the netmask granted wipe cannot wipe") {
    FDB fdb{client_config("FDB_TEST_CATALOGUE_DENIED_PORT")};

    archive_data(fdb);
    EXPECT_EQUAL(count_fields(fdb), fields().size());

    // FDB::wipe() is asynchronous: the request is sent to the servers when the iteration starts. It is refused right
    // away, as not permitted, before any element is produced (nothing is sent to the servers)
    auto it = fdb.wipe(wipe_request(), true);
    WipeElement elem;
    size_t produced = 0;
    std::string error;
    try {
        while (it.next(elem)) {
            ++produced;
        }
    }
    catch (const eckit::UserError& e) {
        error = e.what();
    }
    eckit::Log::info() << "wipe produced " << produced << " elements, failed with: " << error << std::endl;
    EXPECT_EQUAL(produced, 0);
    // refused by the server, for this client
    EXPECT(error.find("is not permitted on FDB server") != std::string::npos);
    EXPECT(error.find("the server does not allow it for this client") != std::string::npos);

    // Nothing has been wiped, neither in the catalogue nor in the store, and the other operations are still available
    EXPECT_EQUAL(count_fields(fdb), fields().size());
    for (const auto& key : fields()) {
        EXPECT_EQUAL(retrieve(fdb, key), k_data);
    }
}

CASE("Remote wipe: a client whose own configuration disables wipe is told so, wherever it connects") {
    Config config = client_config("FDB_TEST_CATALOGUE_ALLOWED_PORT");  // the server would allow it
    config.set("wipe", false);
    FDB fdb{config};

    auto it = fdb.wipe(wipe_request(), true);
    WipeElement elem;
    std::string error;
    try {
        while (it.next(elem)) {}
    }
    catch (const eckit::UserError& e) {
        error = e.what();
    }
    eckit::Log::info() << "wipe failed with: " << error << std::endl;
    EXPECT(error.find("is disabled in the client configuration") != std::string::npos);
}

//-----------------------------------------------------------------------------

}  // namespace fdb5::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}

// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// Verifies that the server-side `networks` configuration restricts wipe to the clients of a given netmask (FDB-663).
///
/// The environment (two catalogue/store server pairs) is configured by an external script. See wipe_netmask.sh.in.
///  - "allowed" servers grant wipe to the netmask containing the test client (127.0.0.0/8) and deny everybody else
///  - "denied" servers grant wipe only to a netmask not containing the test client (10.0.0.0/8)

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Key.h"
#include "fdb5/remote/client/Client.h"

#include "eckit/filesystem/PathName.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"

#include <cstdlib>
#include <string>

using namespace eckit::testing;

namespace fdb5::test {

//-----------------------------------------------------------------------------

namespace {

constexpr auto* k_wipe_request = "class=od,expver=xxxx";
constexpr size_t k_fields = 4;

Config client_config(const char* envVar) {
    const char* path = std::getenv(envVar);
    ASSERT_MSG(path, std::string("Environment variable not set: ") + envVar);
    return Config::make(eckit::PathName{path});
}

void archive_data(FDB& fdb) {
    Key key;
    key.set("class", "od");
    key.set("expver", "xxxx");
    key.set("stream", "oper");
    key.set("time", "0000");
    key.set("domain", "g");
    key.set("levtype", "sfc");
    key.set("param", "167");
    key.set("type", "fc");

    const std::string data = "wipe-netmask-test-data";
    for (const auto* date : {"20101010", "20111213"}) {
        key.set("date", date);
        for (const auto* step : {"1", "2"}) {
            key.set("step", step);
            fdb.archive(key, data.data(), data.size());
        }
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

size_t wipe_all(FDB& fdb) {
    size_t count = 0;
    auto it = fdb.wipe(FDBToolRequest::requestsFromString(k_wipe_request)[0], true);
    WipeElement elem;
    while (it.next(elem)) {
        ++count;
    }
    return count;
}

}  // namespace

//-----------------------------------------------------------------------------

CASE("Remote wipe: client within the netmask granted wipe can wipe") {
    FDB fdb{client_config("FDB_TEST_CLIENT_ALLOWED")};

    archive_data(fdb);
    EXPECT_EQUAL(count_fields(fdb), k_fields);

    EXPECT(wipe_all(fdb) > 0);
    EXPECT_EQUAL(count_fields(fdb), 0);
}

CASE("Remote wipe: client outside the netmask granted wipe cannot wipe") {
    FDB fdb{client_config("FDB_TEST_CLIENT_DENIED")};

    archive_data(fdb);
    EXPECT_EQUAL(count_fields(fdb), k_fields);

    EXPECT_THROWS_AS(wipe_all(fdb), fdb5::remote::RemoteFDBException);

    // Nothing has been wiped, and the other operations are still available to the client
    EXPECT_EQUAL(count_fields(fdb), k_fields);
}

//-----------------------------------------------------------------------------

}  // namespace fdb5::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}

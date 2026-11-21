// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

// An error raised by the remote FDB server while archiving (here: its root is read-only) must be propagated to the
// client, and the same server must be usable again as soon as the problem is fixed.
//
// Run by test_server.sh.in, which exports the roots of the catalogue and store servers.

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/database/Key.h"

#include "metkit/mars/MarsRequest.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/io/Buffer.h"
#include "eckit/io/DataHandle.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"

#include <sys/stat.h>
#include <unistd.h>

#include <cstdlib>
#include <memory>
#include <string>

using namespace eckit::testing;

namespace fdb5::test {

namespace {

const std::string data = "It's gonna be a bright, sunshiny day!";

// The date is part of the database key: each case uses its own, so that the database is created while the root is
// read-only
Key makeKey(const std::string& date) {
    Key k;
    k.set("class", "od");
    k.set("expver", "xxxx");
    k.set("stream", "oper");
    k.set("time", "0000");
    k.set("domain", "g");
    k.set("levtype", "sfc");
    k.set("param", "167");
    k.set("date", date);
    k.set("type", "fc");
    k.set("step", "1");
    return k;
}

metkit::mars::MarsRequest makeRequest(const std::string& date) {
    metkit::mars::MarsRequest req;
    for (const auto& [key, value] : makeKey(date)) {
        req.setValue(key, value);
    }
    return req;
}

std::string rootFromEnvironment(const char* variable) {
    const char* root = std::getenv(variable);
    ASSERT(root != nullptr);
    return root;
}

void setWritable(const std::string& root, bool writable) {
    ASSERT(::chmod(root.c_str(), writable ? 0755 : 0555) == 0);
}

// Leaves the root writable whatever happens, so that the script can clean up after a failure
class ReadOnlyRoot {
public:

    explicit ReadOnlyRoot(std::string root) : root_(std::move(root)) { setWritable(root_, false); }
    ~ReadOnlyRoot() { ::chmod(root_.c_str(), 0755); }

    void makeWritable() { setWritable(root_, true); }

private:

    std::string root_;
};

void testReadOnlyRoot(const char* rootVariable, const std::string& date) {

    // Permissions are not enforced for root
    if (::geteuid() == 0) {
        eckit::Log::info() << "[CLIENT] running as root, permissions are not enforced: skipping" << std::endl;
        return;
    }

    ReadOnlyRoot root(rootFromEnvironment(rootVariable));

    // -- The error of the server is propagated to the client
    {
        eckit::Log::info() << "[CLIENT] archiving to a read-only root: " << rootVariable << std::endl;
        FDB fdb{};

        std::string error;
        try {
            fdb.archive(makeKey(date), data.data(), data.size());
            fdb.flush();
        }
        catch (const eckit::Exception& e) {
            error = e.what();
        }

        eckit::Log::info() << "[CLIENT] error received: " << error << std::endl;
        // The reason of the server, not just a lost connection
        EXPECT(error.find("mkdir") != std::string::npos);
    }

    // -- Once the root is writable the same server archives and serves the field
    root.makeWritable();
    {
        eckit::Log::info() << "[CLIENT] archiving to a writable root: " << rootVariable << std::endl;
        FDB fdb{};
        fdb.archive(makeKey(date), data.data(), data.size());
        fdb.flush();
    }

    {
        FDB fdb{};
        std::unique_ptr<eckit::DataHandle> dh(fdb.retrieve(makeRequest(date)));
        ASSERT(dh);

        eckit::AutoClose closer(*dh);
        dh->openForRead();

        eckit::Buffer buffer(data.size());
        EXPECT_EQUAL(dh->read(buffer, buffer.size()), static_cast<long>(data.size()));
        EXPECT_EQUAL(std::string(static_cast<const char*>(buffer), buffer.size()), data);
    }
}

}  // namespace

//-----------------------------------------------------------------------------

CASE("Remote FDB: an error of the store is propagated to the client, and archival works once it is fixed") {
    testReadOnlyRoot("FDB_REMOTE_STORE_ROOT", "20000101");
}

CASE("Remote FDB: an error of the catalogue is propagated to the client, and archival works once it is fixed") {
    testReadOnlyRoot("FDB_REMOTE_CATALOGUE_ROOT", "20000102");
}

}  // namespace fdb5::test

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}

/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

#include <string>
#include "eckit/testing/Test.h"
#include "eckit/types/Date.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

using namespace eckit::testing;

namespace fdb5::test {

//-----------------------------------------------------------------------------
// Note: the environment for this test is configured by an external script. See tests/remote/test_server.sh.in
// Fairly limited set of tests designed to add some coverage to the client-server comms.
// Tests added to this file should be for behaviour that is expected to work for all server configurations.

std::vector<Key> write_data(FDB& fdb, const std::string& data, const std::string& start_date, int Ndates,
                            int start_step, int Nsteps) {
    // write a few fields
    std::vector<Key> keys;
    Key k;
    k.set("class", "od");
    k.set("expver", "xxxx");
    k.set("stream", "oper");
    k.set("time", "0000");
    k.set("type", "fc");
    k.set("domain", "g");
    k.set("levtype", "sfc");
    k.set("param", "167");

    eckit::Date date{start_date};
    for (int i = 0; i < Ndates; i++) {
        k.set("date", std::to_string(date.yyyymmdd()));
        ++date;
        int step = start_step;
        for (int j = 0; j < Nsteps; j++) {
            k.set("step", std::to_string(step++));
            eckit::Log::info() << " archiving " << k << std::endl;
            fdb.archive(k, data.data(), data.size());
            keys.push_back(k);
        }
    }

    fdb.flush();

    return keys;
}

metkit::mars::MarsRequest make_request(const std::vector<Key>& keys) {
    metkit::mars::MarsRequest req;
    for (const auto& [key, value] : keys[0]) {
        req.setValue(key, value);
    }

    // combine the dates and steps into a single request
    std::vector<std::string> dates;
    std::vector<std::string> steps;
    for (const auto& k : keys) {
        dates.push_back(k.get("date"));
        steps.push_back(k.get("step"));
    }

    req.values("date", dates);
    req.values("step", steps);

    return req;
}

CASE("Remote protocol: the basics") {

    FDB fdb{};  // Expects the config to be set in the environment

    // -- write a few fields
    const size_t Nfields          = 9;
    const std::string data_string = "It's gonna be a bright, sunshiny day!";
    std::vector<Key> keys{};
    { keys = write_data(fdb, data_string, "20000101", 3, 0, 3); }
    EXPECT_EQUAL(keys.size(), Nfields);

    // -- list all fields - use a temporary FDB instance to test if the RemoteFDb life is extended
    auto it = FDB{}.list(FDBToolRequest{{}, true, {}}, false);

    ListElement elem;
    size_t count = 0;
    while (it.next(elem)) {
        eckit::Log::info() << elem << " " << elem.location() << std::endl;
        count++;
    }
    std::cout << "counted " << count << " fields" << std::endl;
    EXPECT(count >= Nfields);

    // -- list all fields - use a temporary FDB instance to test if the RemoteFDb life is extended
    it = FDB{}.list(FDBToolRequest{make_request(keys)}, true);

    count = 0;
    while (it.next(elem)) {
        eckit::Log::info() << elem << " " << elem.location() << std::endl;
        count++;
    }
    EXPECT_EQUAL(count, Nfields);

    // -- retrieve all fields - use a temporary FDB instance to test if the RemoteFDb life is extended
    std::unique_ptr<eckit::DataHandle> dh(FDB{}.retrieve(make_request(keys)));

    eckit::AutoClose closer(*dh);
    dh->openForRead();
    for (int i = 0; i < Nfields; ++i) {
        eckit::Buffer buffer(data_string.size());
        dh->read(buffer, buffer.size());
        auto retrieved_string = std::string((char*)buffer, buffer.size());
        EXPECT_EQUAL(retrieved_string, data_string);
    }

    /// @todo: add wipe when it is implemented.
}

}  // namespace fdb5::test

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}

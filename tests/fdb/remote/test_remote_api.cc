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
#include "eckit/exception/Exceptions.h"
#include "eckit/testing/Test.h"
#include "eckit/types/Date.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/WipeIterator.h"

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

std::vector<Key> keys_level_1(const std::string& start_date, int Ndates) {

    std::vector<Key> keys;
    Key k;
    k.set("class", "od");
    k.set("expver", "xxxx");
    k.set("stream", "oper");
    k.set("time", "0000");
    k.set("domain", "g");

    eckit::Date date{start_date};
    for (int i = 0; i < Ndates; i++) {
        k.set("date", std::to_string(date.yyyymmdd()));
        keys.push_back(k);
    }

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
        auto [_, has_step] = k.find("step");
        if (has_step)
            steps.push_back(k.get("step"));
    }

    req.values("date", dates);
    if (!steps.empty()) {
        req.values("step", steps);
    }

    return req;
}

CASE("Remote protocol: the basics") {

    FDB fdb{};  // Expects the config to be set in the environment

    // -- write a few fields
    const size_t Nfields          = 9;
    const std::string data_string = "It's gonna be a bright, sunshiny day!";
    std::vector<Key> keys         = write_data(fdb, data_string, "20000101", 3, 0, 3);
    EXPECT_EQUAL(keys.size(), Nfields);

    // -- list all fields
    auto it = fdb.list(FDBToolRequest{{}, true, {}});

    ListElement elem;
    size_t count = 0;
    while (it.next(elem)) {
        eckit::Log::info() << elem << " " << elem.location() << std::endl;
        count++;
    }
    EXPECT_EQUAL(count, Nfields);

    // -- retrieve all fields
    std::unique_ptr<eckit::DataHandle> dh(fdb.retrieve(make_request(keys)));

    eckit::AutoClose closer(*dh);
    dh->openForRead();
    for (int i = 0; i < Nfields; ++i) {
        eckit::Buffer buffer(data_string.size());
        dh->read(buffer, buffer.size());
        auto retrieved_string = std::string((char*)buffer, buffer.size());
        EXPECT_EQUAL(retrieved_string, data_string);
    }

    auto k1     = keys_level_1("20000101", 1);
    auto wipeit = fdb.wipe(make_request(k1));

    WipeElement wipeElem;
    std::map<WipeElementType, size_t> element_counts;
    while (wipeit.next(wipeElem)) {
        eckit::Log::info() << wipeElem;
        element_counts[wipeElem.type()] += wipeElem.uris().size();
    }
    // Expect: 1 store .data, 1 catalogue .index, 2 catalogue files (schema, toc)
    EXPECT_EQUAL(element_counts[WipeElementType::STORE], 1);
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 1);
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 2);

    // -- list all fields
    it = fdb.list(FDBToolRequest{{}, true, {}});

    count = 0;
    while (it.next(elem)) {
        eckit::Log::info() << elem << " " << elem.location() << std::endl;
        count++;
    }
    EXPECT_EQUAL(count, Nfields);

    // Wipe, with doit=true
    wipeit = fdb.wipe(make_request(k1), true);
    element_counts.clear();
    while (wipeit.next(wipeElem)) {
        eckit::Log::info() << wipeElem;
        element_counts[wipeElem.type()] += wipeElem.uris().size();
    }

    // Expect: 1 store .data, 1 catalogue .index, 2 catalogue files (schema, toc)
    EXPECT_EQUAL(element_counts[WipeElementType::STORE], 1);
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 1);
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 2);

    // -- list all remaining fields
    it = fdb.list(FDBToolRequest{{}, true, {}});

    count = 0;
    while (it.next(elem)) {
        eckit::Log::info() << elem << " " << elem.location() << std::endl;
        count++;
    }
    EXPECT_EQUAL(count, 6);

    // Wipe everything that remains
    wipeit = fdb.wipe(FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0], true);
    count  = 0;
    while (wipeit.next(wipeElem)) {
        eckit::Log::info() << wipeElem;
        count++;
    }
}

CASE("Remote protocol: more wipe testing") {

    FDB fdb{};  // Expects the config to be set in the environment

    // -- write a few fields. 2 databases
    const size_t Nfields          = 6;
    const std::string data_string = "It's gonna be a bright, sunshiny day!";
    std::vector<Key> keys         = write_data(fdb, data_string, "20000101", 2, 0, 3);
    EXPECT_EQUAL(keys.size(), Nfields);

    auto wipeit = fdb.wipe(FDBToolRequest::requestsFromString("class=od,expver=xxxx,date=20000101")[0]);
    WipeElement wipeElem;
    std::map<WipeElementType, size_t> element_counts;
    while (wipeit.next(wipeElem)) {
        eckit::Log::info() << wipeElem;
        element_counts[wipeElem.type()] += wipeElem.uris().size();
    }

    // Expect: 1 store .data, 1 catalogue .index, 2 catalogue files (schema, toc)
    EXPECT_EQUAL(element_counts[WipeElementType::STORE], 1);
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 1);
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 2);

    // Wipe both dates
    wipeit = fdb.wipe(FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0], false); // dont actually delete anything
    element_counts.clear();

    std::vector<eckit::URI> store_uris;
    while (wipeit.next(wipeElem)) {
        eckit::Log::info() << wipeElem;
        element_counts[wipeElem.type()] += wipeElem.uris().size();
        if (wipeElem.type() == WipeElementType::STORE) {
            store_uris.insert(store_uris.end(), wipeElem.uris().begin(), wipeElem.uris().end());
        }
    }

    // Expect: 2x (1 store .data, 1 catalogue .index, 2 catalogue files (schema, toc))
    EXPECT_EQUAL(element_counts[WipeElementType::STORE], 2);
    EXPECT_EQUAL(element_counts[WipeElementType::STORE_AUX], 0);
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 2);
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 4);


    // artifically add some auxiliary .gribjump files to the store.
    for (const auto& store_uri : store_uris) {
        if (!(store_uri.path().extension() == ".data")) {
            continue;
        }

        eckit::PathName p = store_uri.path();
        p += ".gribjump";
        eckit::FileHandle fh(p);
        fh.openForWrite(0);
        std::string junk = "junk";
        fh.write(junk.data(), junk.size());
        fh.close();

    }

    wipeit = fdb.wipe(FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0], true); // doit
    element_counts.clear();

    while (wipeit.next(wipeElem)) {
        element_counts[wipeElem.type()] += wipeElem.uris().size();
        eckit::Log::info() << wipeElem;
    }

    // Expect: 2x (1 store .data, 1 catalogue .index, 2 catalogue files (schema, toc))
    // and also auxiliary files for each .data file
    EXPECT_EQUAL(element_counts[WipeElementType::STORE], 2);
    EXPECT_EQUAL(element_counts[WipeElementType::STORE_AUX], 2);
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 2);
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 4);

    // there should be nothing left.
    wipeit = fdb.wipe(FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0], false);
    element_counts.clear();

    while (wipeit.next(wipeElem)) {
        element_counts[wipeElem.type()] += wipeElem.uris().size();
        eckit::Log::info() << wipeElem;
    }

    EXPECT_EQUAL(element_counts[WipeElementType::STORE], 0);
    EXPECT_EQUAL(element_counts[WipeElementType::STORE_AUX], 0);
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 0);
    EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 0);
}

}  // namespace fdb5::test

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}

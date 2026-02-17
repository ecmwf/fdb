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
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/api/helpers/WipeIterator.h"

using namespace eckit::testing;

namespace fdb5::test {

//-----------------------------------------------------------------------------
// Note: the environment for this test is configured by an external script. See tests/remote/test_server.sh.in
// Fairly limited set of tests designed to add some coverage to the client-server comms.
// Tests added to this file should be for behaviour that is expected to work for all server configurations.

// Note: I suggest, as a convention, to prepend all logs with [CLIENT] to make it easier to
// distinguish client and server logs in the test output.

Key keycommon(int level = 3) {
    Key k;
    // level 1
    k.set("class", "od");
    k.set("expver", "xxxx");
    k.set("stream", "oper");
    k.set("time", "0000");
    k.set("domain", "g");
    // level 2
    if (level >= 2) {
        k.set("levtype", "sfc");
    }
    // level 3
    if (level >= 3) {
        k.set("param", "167");
    }
    return k;
}

std::vector<Key> write_data(FDB& fdb, const std::string& data,
                            // const std::string& start_date, int Ndates, int start_step, int Nsteps
                            const std::vector<std::string>& dates = {"20101010", "20111213"},
                            const std::vector<std::string>& types = {"fc", "pf"},
                            const std::vector<std::string>& steps = {"1", "2"}) {
    // write a few fields
    std::vector<Key> keys;
    Key k = keycommon();

    for (const auto& date : dates) {
        k.set("date", date);
        for (const auto& type : types) {
            k.set("type", type);
            for (const auto& step : steps) {
                k.set("step", step);
                eckit::Log::info() << "[CLIENT]" << " archiving " << k << std::endl;
                fdb.archive(k, data.data(), data.size());
                keys.push_back(k);
            }
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
    std::set<std::string> dates;
    std::set<std::string> types;
    std::set<std::string> steps;
    for (const auto& k : keys) {
        dates.insert(k.get("date"));
        types.insert(k.get("type"));
        steps.insert(k.get("step"));
    }

    req.values("date", std::vector<std::string>(dates.begin(), dates.end()));
    req.values("type", std::vector<std::string>(types.begin(), types.end()));
    req.values("step", std::vector<std::string>(steps.begin(), steps.end()));

    return req;
}

// Note: The catalogue server is configured to use subtocs. This means there will be cleared indexes and subtocs.
// This means we also must be sure to disconnect between calls inorder to see the consolidated .index file.

CASE("Remote protocol: the basics") {


    // -- write a few fields
    const size_t Nfields          = 8;
    const std::string data_string = "It's gonna be a bright, sunshiny day!";
    std::vector<Key> keys;
    {
        FDB fdb{};  // Expects the config to be set in the environment
        keys = write_data(fdb, data_string, {"20000101", "20000102"}, {"fc", "pf"}, {"1", "2"});
    }
    EXPECT_EQUAL(keys.size(), Nfields);
    std::this_thread::sleep_for(std::chrono::seconds(2));  // Ensure server has time to flush consolidated indexes.

    // -- list all fields -
    size_t count = 0;
    {
        auto it = FDB{}.list(FDBToolRequest{{}, true, {}}, true);
        ListElement elem;
        while (it.next(elem)) {
            eckit::Log::info() << elem << " " << elem.location() << std::endl;
            count++;
        }
        EXPECT_EQUAL(count, Nfields);
    }

    // -- list all fields - use a temporary FDB instance to test if the RemoteFDb life is extended
    {

        count   = 0;
        auto it = FDB{}.list(FDBToolRequest{make_request(keys)}, true);
        ListElement elem;
        while (it.next(elem)) {
            eckit::Log::info() << "[CLIENT]" << elem << " " << elem.location() << std::endl;
            count++;
        }
        EXPECT_EQUAL(count, Nfields);
    }

    // -- retrieve all fields -
    {
        std::unique_ptr<eckit::DataHandle> dh(FDB{}.retrieve(make_request(keys)));

        eckit::AutoClose closer(*dh);
        dh->openForRead();
        for (int i = 0; i < Nfields; ++i) {
            eckit::Buffer buffer(data_string.size());
            dh->read(buffer, buffer.size());
            auto retrieved_string = std::string((char*)buffer, buffer.size());
            EXPECT_EQUAL(retrieved_string, data_string);
        }
    }

    // -- wipe (doit=false) --
    {
        auto wipeit = FDB{}.wipe(FDBToolRequest::requestsFromString("date=20000101")[0]);
        WipeElement wipeElem;
        std::map<WipeElementType, size_t> element_counts;
        while (wipeit.next(wipeElem)) {
            eckit::Log::info() << "[CLIENT]" << wipeElem;
            element_counts[wipeElem.type()] += wipeElem.uris().size();
        }
        // Expect: 2 store .data, 4 catalogue .index, 3 catalogue files (schema, toc, subtoc)
        EXPECT_EQUAL(element_counts[WipeElementType::STORE], 2);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 4);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 3);
        EXPECT_EQUAL(element_counts[WipeElementType::UNKNOWN], 0);
    }

    // -- list all fields again, expect same number as before
    {
        count   = 0;
        auto it = FDB{}.list(FDBToolRequest{{}, true, {}}, true);
        ListElement elem;
        while (it.next(elem)) {
            eckit::Log::info() << "[CLIENT]" << elem << " " << elem.location() << std::endl;
            count++;
        }
        EXPECT_EQUAL(count, Nfields);
    }

    // -- list fields, expect same number. --
    {

        auto it = FDB{}.list(FDBToolRequest{make_request(keys)}, true);
        ListElement elem;
        count = 0;
        while (it.next(elem)) {
            eckit::Log::info() << "[CLIENT]" << elem << " " << elem.location() << std::endl;
            count++;
        }
        EXPECT_EQUAL(count, Nfields);
    }

    // Wipe, with doit=true
    {

        auto wipeit = FDB{}.wipe(FDBToolRequest::requestsFromString("date=20000101")[0], true);
        WipeElement wipeElem;
        std::map<WipeElementType, size_t> element_counts;
        while (wipeit.next(wipeElem)) {
            eckit::Log::info() << "[CLIENT]" << wipeElem;
            element_counts[wipeElem.type()] += wipeElem.uris().size();
        }

        // Expect: 2 store .data, 2 catalogue .index, 3 catalogue files (schema, toc, subtoc)
        EXPECT_EQUAL(element_counts[WipeElementType::STORE], 2);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 4);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 3);
        EXPECT_EQUAL(element_counts[WipeElementType::UNKNOWN], 0);
    }

    // -- list all remaining fields
    {
        count   = 0;
        auto it = FDB{}.list(FDBToolRequest{{}, true, {}}, true);
        ListElement elem;
        while (it.next(elem)) {
            eckit::Log::info() << "[CLIENT]" << elem << " " << elem.location() << std::endl;
            count++;
        }
        EXPECT_EQUAL(count, 4);
    }

    // Wipe everything that remains
    {
        auto wipeit = FDB{}.wipe(FDBToolRequest::requestsFromString("class=od")[0], true);
        WipeElement wipeElem;
        count = 0;
        while (wipeit.next(wipeElem)) {
            eckit::Log::info() << "[CLIENT]" << wipeElem;
            count++;
        }
    }
}

CASE("Remote protocol: more wipe testing") {

    // Before starting this test, ensure there is no data current in the FDB (e.g. from the previous test...)
    {
        auto wipeit = FDB{}.wipe(FDBToolRequest::requestsFromString("class=od")[0], false);
        WipeElement wipeElem;
        std::map<WipeElementType, size_t> element_counts;

        while (wipeit.next(wipeElem)) {
            element_counts[wipeElem.type()] += wipeElem.uris().size();
            eckit::Log::info() << "[CLIENT]" << wipeElem;
        }

        EXPECT_EQUAL(element_counts[WipeElementType::STORE], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::STORE_AUX], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::UNKNOWN], 0);
    }


    // -- write a few fields. 2 databases. Each with 1 index and 1 data file
    const size_t Nfields          = 8;
    const std::string data_string = "It's gonna be a bright, sunshiny day!";
    std::vector<Key> keys;
    {
        FDB fdb{};  // Expects the config to be set in the environment
        keys = write_data(fdb, data_string, {"20000101", "20000102"}, {"fc", "pf"}, {"1", "2"});
    }
    EXPECT_EQUAL(keys.size(), Nfields);
    std::this_thread::sleep_for(std::chrono::seconds(2));  // Ensure server has time to flush consolidated indexes.

    // wipe a single date
    {
        auto wipeit = FDB{}.wipe(FDBToolRequest::requestsFromString("class=od,expver=xxxx,date=20000101")[0]);
        WipeElement wipeElem;
        std::map<WipeElementType, size_t> element_counts;
        while (wipeit.next(wipeElem)) {
            eckit::Log::info() << "[CLIENT]" << wipeElem;
            element_counts[wipeElem.type()] += wipeElem.uris().size();
        }

        // Expect: 2 store .data, 2 catalogue .index, 2 catalogue files (schema, toc)
        EXPECT_EQUAL(element_counts[WipeElementType::STORE], 2);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 4);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 3);
        EXPECT_EQUAL(element_counts[WipeElementType::UNKNOWN], 0);
    }
    // Duplicate everything
    {
        FDB fdb{};
        write_data(fdb, data_string, {"20000101", "20000102"}, {"fc", "pf"}, {"1", "2"});
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));  // Ensure server has time to flush consolidated indexes.

    // Wipe just one DB (date=20000101)
    std::vector<eckit::URI> data_uris;
    std::vector<eckit::URI> index_uris;
    {
        auto wipeit = FDB{}.wipe(FDBToolRequest::requestsFromString("class=od,expver=xxxx,date=20000101")[0],
                                 false);  // dont actually delete anything

        WipeElement wipeElem;
        std::map<WipeElementType, size_t> element_counts;

        while (wipeit.next(wipeElem)) {
            eckit::Log::info() << "[CLIENT]" << wipeElem;
            element_counts[wipeElem.type()] += wipeElem.uris().size();
            if (wipeElem.type() == WipeElementType::STORE) {
                data_uris.insert(data_uris.end(), wipeElem.uris().begin(), wipeElem.uris().end());
            }
            else if (wipeElem.type() == WipeElementType::CATALOGUE_INDEX) {
                index_uris.insert(index_uris.end(), wipeElem.uris().begin(), wipeElem.uris().end());
            }
        }

        // Expect: 4 .data files, 4 .index files and 4 catalogue files (2 schema, 2 toc)
        EXPECT_EQUAL(element_counts[WipeElementType::STORE], 4);
        EXPECT_EQUAL(element_counts[WipeElementType::STORE_AUX], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 8);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 4);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_SAFE], 0);  // full db wipe, so none safe
        EXPECT_EQUAL(element_counts[WipeElementType::UNKNOWN], 0);

        // artifically add some auxiliary .gribjump files to the store for this db.
        for (const auto& store_uri : data_uris) {
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
    }
    {
        // Partial wipe on level 2 (type=fc). Should hit half the index/data files
        auto wipeit = FDB{}.wipe(FDBToolRequest::requestsFromString("class=od,expver=xxxx,type=fc")[0], false);
        WipeElement wipeElem;
        std::map<WipeElementType, size_t> element_counts;
        while (wipeit.next(wipeElem)) {
            element_counts[wipeElem.type()] += wipeElem.uris().size();
            eckit::Log::info() << "[CLIENT]" << wipeElem;
        }
        EXPECT_EQUAL(element_counts[WipeElementType::STORE], 4);
        EXPECT_EQUAL(element_counts[WipeElementType::STORE_AUX], 2);  // the .gribjump files
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 4);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 0);  // no DBs are fully wiped
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_SAFE],
                     8);  // the 4 un-cleared indexes, and the two toc/schemas
        EXPECT_EQUAL(element_counts[WipeElementType::UNKNOWN], 0);
    }

    // Over specified wipe on level 3 (step=1). Should mark nothing for deletion.
    {
        auto wipeit = FDB{}.wipe(FDBToolRequest::requestsFromString("class=od,expver=xxxx,step=1")[0], false);

        WipeElement wipeElem;
        std::map<WipeElementType, size_t> element_counts;
        while (wipeit.next(wipeElem)) {
            element_counts[wipeElem.type()] += wipeElem.uris().size();
            eckit::Log::info() << "[CLIENT]" << wipeElem;
        }
        EXPECT_EQUAL(element_counts[WipeElementType::STORE], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::STORE_AUX], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::UNKNOWN], 0);
    }

    // Now actually wipe everything
    {
        auto wipeit = FDB{}.wipe(FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0], true);  // doit
        WipeElement wipeElem;
        std::map<WipeElementType, size_t> element_counts;

        while (wipeit.next(wipeElem)) {
            element_counts[wipeElem.type()] += wipeElem.uris().size();
            eckit::Log::info() << "[CLIENT]" << wipeElem;
        }

        EXPECT_EQUAL(element_counts[WipeElementType::STORE], 8);
        EXPECT_EQUAL(element_counts[WipeElementType::STORE_AUX], 4);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 16);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 8);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_SAFE], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::UNKNOWN], 0);
    }

    // there should be nothing left.
    {
        auto wipeit = FDB{}.wipe(FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0], false);
        WipeElement wipeElem;
        std::map<WipeElementType, size_t> element_counts;

        while (wipeit.next(wipeElem)) {
            element_counts[wipeElem.type()] += wipeElem.uris().size();
            eckit::Log::info() << "[CLIENT]" << wipeElem;
        }

        EXPECT_EQUAL(element_counts[WipeElementType::STORE], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::STORE_AUX], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE_INDEX], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::CATALOGUE], 0);
        EXPECT_EQUAL(element_counts[WipeElementType::UNKNOWN], 0);
    }

    // Make sure none of the directories are left behind
    for (const auto& store_uri : data_uris) {
        eckit::PathName p = store_uri.path().dirName();
        EXPECT(!p.exists());
    }

    for (const auto& index_uri : index_uris) {
        eckit::PathName p = index_uri.path().dirName();
        EXPECT(!p.exists());
    }
}

}  // namespace fdb5::test

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}

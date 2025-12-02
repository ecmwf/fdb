/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cstdlib>
#include <iostream>

#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/testing/Test.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

using namespace eckit::testing;
using namespace eckit;

namespace fdb5::test {

//----------------------------------------------------------------------------------------------------------------------

int count_in_list(FDB& fdb, const std::string& request_str) {
    int count = 0;
    auto listObject = fdb.list(FDBToolRequest::requestsFromString(request_str, {})[0]);
    ListElement elem;
    while (listObject.next(elem)) {
        count++;
    }
    return count;
}

int count_in_inspect(FDB& fdb, const std::string& request_str) {
    int count = 0;

    // prepend base request:
    std::string base =
        "retrieve,class=od,stream=enfo,expver=xxxx,type=pf,date=20000101,domain=g,levtype=sfc,param=167,step=1";

    auto request = metkit::mars::MarsRequest::parse(base + "," + request_str);
    auto inspectObject = fdb.inspect(request);
    ListElement elem;
    while (inspectObject.next(elem)) {
        count++;
    }
    return count;
}

CASE("write") {

    TmpDir root1(LocalPathName::cwd().c_str());
    TmpDir root2(LocalPathName::cwd().c_str());
    TmpDir root3(LocalPathName::cwd().c_str());

    std::vector<std::string> roots = {root1.asString(), root2.asString(), root3.asString()};

    std::ostringstream oss;
    oss << R"XX(
        ---
        type: select
        fdbs:
        - select: time=0000
          excludes: [
            "number=(1|2)",
            "time=1200,number=3"
            ]
          type: local
          spaces:
          - roots:
            - path: )XX"
        << roots[0] << R"XX(
        - select: time=1200
          excludes: ["number=2"]
          type: local
          spaces:
          - roots:
            - path: )XX"
        << roots[1] << R"XX(
        - select: number=(1|2)
          type: local
          spaces:
          - roots:
            - path: )XX"
        << roots[2] << R"XX(
        )XX";

    const std::string config_str = oss.str();
    std::cout << "Using config:\n" << config_str << std::endl;
    eckit::testing::SetEnv env("FDB5_CONFIG", config_str.c_str());

    // ------------------------------------------------------------------------------------------------------
    FDB fdb;
    std::cout << "init fdb." << std::endl;

    // Do some archiving
    std::string data_str = "Let it snow";
    const void* data = static_cast<const void*>(data_str.c_str());
    size_t length = data_str.size();

    Key k;
    k.set("class", "od");
    k.set("expver", "xxxx");
    k.set("type", "pf");
    k.set("date", "20000101");
    k.set("time", "0000");
    k.set("domain", "g");
    k.set("levtype", "sfc");
    k.set("param", "167");
    k.set("step", "1");
    k.set("stream", "enfo");

    // expect:
    // oper,xxxx -> lane 3
    // oper,yyyy -> lane 3
    // oper,zzzz -> lane 1
    // enfo,xxxx -> lane 2
    // enfo,yyyy -> lane 3
    // enfo,zzzz -> lane 2

    auto times = {"0000", "1200"};
    auto numbers = {"1", "2", "3"};

    for (const auto& t : times) {
        for (const auto& n : numbers) {
            k.set("time", t);
            k.set("number", n);
            std::cout << "Archiving for time " << t << " number " << n << std::endl;
            fdb.archive(k, data, length);
        }
    }
    fdb.flush();

    auto listObject = fdb.list(FDBToolRequest(metkit::mars::MarsRequest{}, true, {}));

    std::map<std::string, int> expected_counts = {
        {roots[0], 1},
        {roots[1], 2},
        {roots[2], 3},
    };

    std::map<std::string, int> counts = {
        {roots[0], 0},
        {roots[1], 0},
        {roots[2], 0},
    };

    ListElement elem;
    while (listObject.next(elem)) {
        Log::info() << elem << ", " << elem.uri().path().dirName().dirName() << std::endl;
        std::string root = elem.uri().path().dirName().dirName().asString();
        EXPECT(counts.find(root) != counts.end());
        counts[root]++;
    }

    for (const auto& root : roots) {
        EXPECT_EQUAL(counts[root], expected_counts[root]);
    }

    // ------------------------------------------------------------------------------------------------------
    // test listing with various partial requests

    EXPECT_EQUAL(count_in_list(fdb, "time=0000"), 3);
    EXPECT_EQUAL(count_in_list(fdb, "time=1200"), 3);
    EXPECT_EQUAL(count_in_list(fdb, "time=0000/1200"), 6);
    EXPECT_EQUAL(count_in_list(fdb, "number=1"), 2);
    EXPECT_EQUAL(count_in_list(fdb, "number=2"), 2);
    EXPECT_EQUAL(count_in_list(fdb, "number=3"), 2);
    EXPECT_EQUAL(count_in_list(fdb, "time=0000,number=1/2"), 2);
    EXPECT_EQUAL(count_in_list(fdb, "time=0000,number=2/3"), 2);
    EXPECT_EQUAL(count_in_list(fdb, "time=1200,number=1/2/3"), 3);
    EXPECT_EQUAL(count_in_list(fdb, "number=2/3"), 4);
    EXPECT_EQUAL(count_in_list(fdb, "number=1/3"), 4);
    EXPECT_EQUAL(count_in_list(fdb, "number=1/2/3"), 6);
    auto s = "class=od,stream=enfo,expver=xxxx,type=pf,date=20000101,domain=g,levtype=sfc,param=167,step=1";
    EXPECT_EQUAL(count_in_list(fdb, s), 6);

    // ------------------------------------------------------------------------------------------------------
    EXPECT_EQUAL(count_in_inspect(fdb, "time=1200"), 0);  // inspect does not take partial requests
    EXPECT_EQUAL(count_in_inspect(fdb, "time=1200,number=1"), 1);
    EXPECT_EQUAL(count_in_inspect(fdb, "time=0000,number=1/2"), 2);
    EXPECT_EQUAL(count_in_inspect(fdb, "time=0000,number=2/3"), 2);
    EXPECT_EQUAL(count_in_inspect(fdb, "time=1200,number=1/2/3"), 3);
    EXPECT_EQUAL(count_in_inspect(fdb, "time=0000/1200,number=1/2/3"), 6);
}


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
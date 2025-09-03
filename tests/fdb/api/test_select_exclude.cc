/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include <cstdlib>
#include <iostream>

#include "eckit/testing/Test.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/TmpDir.h"

#include "metkit/mars/MarsRequest.h"
#include "metkit/mars/TypeAny.h"

#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"
#include "fdb5/api/FDB.h"

using namespace eckit::testing;
using namespace eckit;


namespace fdb5 {
namespace test {

//----------------------------------------------------------------------------------------------------------------------

CASE("write") {

    TmpDir root1(LocalPathName::cwd().c_str());
    TmpDir root2(LocalPathName::cwd().c_str());
    TmpDir root3(LocalPathName::cwd().c_str());

    std::vector<std::string> roots = {root1.asString(), root2.asString(), root3.asString()};


    // todo: make one of these lanes have two excludes
    std::ostringstream oss;
    oss << R"XX(
        ---
        type: select
        fdbs:
        - select: stream=oper
          filter:
          - exclude: expver=(xxxx|yyyy)
          - exclude: stream=enfo,expver=zzzz
          type: local
          spaces:
          - roots:
            - path: )XX" << roots[0] << R"XX(
        - select: stream=enfo
          filter:
          - exclude: expver=(yyyy)
          type: local
          spaces:
          - roots:
            - path: )XX" << roots[1] << R"XX(
        - select: expver=(xxxx|yyyy)
          type: local
          spaces:
          - roots:
            - path: )XX" << roots[2] << R"XX(
        )XX";

    const std::string config_str = oss.str();
    std::cout << "Using config:\n" << config_str << std::endl;
    eckit::testing::SetEnv env("FDB5_CONFIG", config_str.c_str());
    std::cout << "Set FDB5_CONFIG." << std::endl;

    // ------------------------------------------------------------------------------------------------------
    FDB fdb;
    std::cout << "init fdb." << std::endl;

    // Do some archiving
    std::string data_str = "Let it snow";
    const void* data     = static_cast<const void*>(data_str.c_str());
    size_t length        = data_str.size();

    Key k;
    k.set("class", "od");
    k.set("expver", "xxxx");
    k.set("type", "fc");
    k.set("stream", "oper");
    k.set("date", "20000101");
    k.set("time", "0000");
    k.set("domain", "g");
    k.set("levtype", "sfc");
    k.set("param", "167");
    k.set("step", "1");

    auto streams = {"oper", "enfo"};
    auto expvers = {"xxxx", "yyyy", "zzzz"};

    for (const auto& s : streams) {
        for (const auto& e : expvers) {
            k.set("stream", s);
            k.set("expver", e);
            std::cout << "Archiving for stream=" << s << ", expver=" << e << std::endl;
            fdb.archive(k, data, length);
        }
    }
    fdb.flush();

    auto listObject = fdb.list(FDBToolRequest(metkit::mars::MarsRequest{}, true, {}));
    
    std::map<std::string, int> counts = {
        {roots[0], 0},
        {roots[1], 0},
        {roots[2], 0},
    };

    ListElement elem;
    while (listObject.next(elem)) {
        Log::info() << elem << ", " << elem.uri().path().dirName().dirName() << std::endl;
        auto root = elem.uri().path().dirName().dirName().asString();
        EXPECT(counts.find(root) != counts.end());
        counts[root]++;
    }


    std::map<std::string, int> expected_counts = {
        {roots[0], 1},
        {roots[1], 2},
        {roots[2], 3},
    };

    for (const auto& root : roots) {
        EXPECT_EQUAL(counts[root], expected_counts[root]);
    }




/* TODO: make sure read and list work as expected
select: class=od
  excludes:
    - expver=(xxxx|yyyy)

select: class=rd
  excludes:
    - expver=yyyy

select: expver=xxxx
  .. # with no excludes

fdb-list class=od/rd               # < lists all 3 lanes.
fdb-list class=od/rd,expver=xxxx   # < lists lanes 2 and 3. 1 is selected but then excluded by expver=xxxx
fdb-list class=od,expver=xxxx      # < lists lane 3. 1 is selected but then excluded.
fdb-list expver=xxxx               # < lists lanes 2 and 3.
fdb-list expver=yyyy               # < lists no lane.
fdb-list expver=xxxx/yyyy          # < should list lanes 2 and 3.
*/

}


//----------------------------------------------------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
//  (C) Copyright 1996- ECMWF.
//
//  This software is licensed under the terms of the Apache Licence Version 2.0
//  which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
//  In applying this licence, ECMWF does not waive the privileges and immunities
//  granted to it by virtue of its status as an intergovernmental organisation nor
//  does it submit to any jurisdiction.
//

// ----------------------------------------------------------------------------------------------------------------------
// Based on FDB-425:
// We found there were problems listing an FDB after purging when the purged fields were recorded in a subtoc.
// These tests recreate this situation and verify that listing now works correctly.
// ----------------------------------------------------------------------------------------------------------------------

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "eckit/filesystem/PathName.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/config/Config.h"

using namespace eckit::testing;
using namespace eckit;


namespace fdb5 {
namespace test {

//----------------------------------------------------------------------------------------------------------------------
std::string foldername = "fdb-425-root";


fdb5::Config theconfig(bool useSubToc = false) {

    std::string config_str =
        "---\n"
        "type: local\n"
        "engine: toc\n"
        "spaces:\n"
        "- roots:\n"
        "  - path: \"./" +
        foldername +
        "\"\n"
        "schema: ./fdb-425-schema\n";

    eckit::testing::SetEnv env("FDB5_CONFIG", config_str.c_str());

    fdb5::Config expanded = fdb5::Config().expandConfig();
    if (!useSubToc) {
        return expanded;
    }

    eckit::LocalConfiguration userConf;
    userConf.set("useSubToc", true);

    fdb5::Config cfg(expanded, userConf);

    return cfg;
}

// Write a bunch of data using Nparam processes. Each fork writes a different param, and Nsteps steps.
void runmodel(size_t Nparam, size_t Nsteps, bool useSubToc = true, bool dofork = true) {
    eckit::Log::info() << "Start runmodel" << std::endl;
    {
        // make fakemodel directory
        eckit::PathName(foldername).mkdir();

        // Do some archiving
        fdb5::Key k;
        k.set("class", "od");
        k.set("expver", "xxxx");
        k.set("stream", "oper");
        k.set("type", "fc");
        k.set("date", "20000101");
        k.set("time", "0000");
        k.set("domain", "g");
        k.set("levtype", "sfc");
        k.set("param", "167");
        std::vector<int> data = {1, 2, 3};

        if (dofork) {
            // Fork once per param
            std::vector<pid_t> pids(Nparam);
            for (int i = 0; i < Nparam; i++) {
                pids[i] = fork();
                ASSERT(pids[i] >= 0);
                if (pids[i] == 0) {  // child
                    fdb5::FDB fdb(theconfig(useSubToc));
                    k.set("param", std::to_string(167 + i));
                    for (int step = 0; step < Nsteps; step++) {
                        k.set("step", std::to_string(step));
                        eckit::Log::info() << " archiving " << k << std::endl;
                        fdb.archive(k, data.data(), data.size());
                    }
                    fdb.flush();
                    exit(0);
                }
            }

            // Parent wait for children
            for (int i = 0; i < Nparam; i++) {
                int status;
                waitpid(pids[i], &status, 0);
                EXPECT(status == 0);
            }
        }
        else {
            // No forking, but write all the same.
            for (int i = 0; i < Nparam; i++) {
                fdb5::FDB fdb(theconfig(useSubToc));
                k.set("param", std::to_string(167 + i));
                for (int step = 0; step < Nsteps; step++) {
                    k.set("step", std::to_string(step));
                    eckit::Log::info() << " archiving " << k << std::endl;
                    fdb.archive(k, data.data(), data.size());
                }
                fdb.flush();
            }
        }
    }
    eckit::Log::info() << "Finish runmodel" << std::endl;
}

// List the contents of the fdb, check that the number of elements is as expected
void list(bool dedup, size_t expected) {
    eckit::Log::info() << "Start list" << std::endl;
    {
        fdb5::FDB fdb(theconfig(false));
        fdb5::FDBToolRequest request = fdb5::FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0];
        auto it = fdb.list(request, dedup);

        ListElement elem;
        size_t count = 0;
        while (it.next(elem)) {
            Log::info() << elem << " " << elem.location() << std::endl;
            count++;
        }

        EXPECT_EQUAL(count, expected);
    }
    eckit::Log::info() << "Finish list" << std::endl;
}

// Purge the fdb
void purge(bool doit) {
    eckit::Log::info() << "Start Purge doit=" << doit << std::endl;
    {
        fdb5::FDB fdb(theconfig(false));
        fdb5::FDBToolRequest request = fdb5::FDBToolRequest::requestsFromString("class=od,expver=xxxx")[0];

        // dont do it first, for logging reasons
        auto purgeIterator = fdb.purge(request, doit, false);
        PurgeElement elem;
        while (purgeIterator.next(elem)) {
            Log::info() << elem << std::endl;
        }
    }
    eckit::Log::info() << "Finish Purge doit=" << doit << std::endl;
}

void cleanup() {
    // remove all contents of dir
    auto td = eckit::PathName(foldername);
    if (td.exists()) {
        std::vector<eckit::PathName> files;
        std::vector<eckit::PathName> dirs;
        td.childrenRecursive(files, dirs);
        for (auto f : files) {
            f.unlink();
        }
        for (auto d : dirs) {
            d.rmdir();
        }
        eckit::PathName(foldername).rmdir();  //< only removes if it is empty
    }
}

// Case: Full subtoc purging
// Future note: We could add TOC_CLEAR to the sub toc entry instead but we currently do not (we clear one index at a
// time)
CASE("FDB-425: Archive, rearchive, purge, then list.") {

    cleanup();

    size_t Nparam = 2;
    size_t Nsteps = 3;
    size_t Nunique = Nsteps * Nparam;

    size_t Nruns = 3;
    for (int i = 0; i < Nruns; i++) {
        runmodel(Nparam, Nsteps);
    }

    // Verify fdb list works pre-purge: 9 masked 9 unmasked
    list(true, Nunique);
    list(false, Nruns * Nunique);

    purge(true);

    // Rerun the list, correct behaviour would be 0 masked 9 unmasked and dedupe should do nothing
    list(true, Nunique);
    list(false, Nunique);  //< i.e. all duplicates are gone
}

// Case: When we cannot purge the entire index, we should not purge anything
///@todo: This test has issues (hang on fdb.write) when forking (on linux but not macos), so we disable forking for now.
// this means no subtocs are written, so it is a less interesting test for now.
CASE("Check more finer-grained purge behaviour (note no forks)") {

    cleanup();

    // Initial run
    size_t Nparam = 2;  // == Nparams
    size_t Nsteps = 3;
    size_t Nunique = Nsteps * Nparam;
    bool subtocs = true;
    bool dofork = false;
    runmodel(Nparam, Nsteps, subtocs, dofork);

    // Rerun #1: rewrite one step for every param
    runmodel(Nparam, 1, subtocs, dofork);

    // Expect 1 duplicate per param
    list(true, Nunique);
    list(false, Nunique + Nparam);

    // Because each index will contain Nsteps entries, purge cannot delete any of them
    // So listing will not change
    purge(true);
    list(true, Nunique);
    list(false, Nunique + Nparam);

    // Rerun #2: rewrite first step for every param, thus masking Rerun #1 which can be purged
    runmodel(Nparam, 1, subtocs, dofork);
    list(false, Nunique + 2 * Nparam);
    purge(true);
    list(false, Nunique + Nparam);

    // Rerun #3: Rerun everything. With everything masked we can purge all duplicates
    runmodel(Nparam, Nsteps, subtocs, dofork);
    purge(true);
    list(true, Nunique);
    list(false, Nunique);  //< i.e. all duplicates are gone
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb5

int main(int argc, char** argv) {
    int err = run_tests(argc, argv);
    if (err) {
        return err;
    }
    // fdb5::test::cleanup();
    return 0;
}

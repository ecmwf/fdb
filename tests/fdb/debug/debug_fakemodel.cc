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

#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "eckit/filesystem/PathName.h"
#include "eckit/log/Log.h"
#include "eckit/testing/Test.h"

#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/FDB.h"

using namespace eckit::testing;
using namespace eckit;


namespace fdb5 {
namespace test {
 
//----------------------------------------------------------------------------------------------------------------------

std::string foldername = "debug_root";
constexpr size_t Nforks  = 2;              // Number of processes to fork (~ number of subtocs)
constexpr size_t Nsteps  = 3;              // Number of fields written by each forked process (~ number in each subtoc)
constexpr size_t Nunique = Nsteps*Nforks;  // Number of fields written by the "model"
constexpr size_t Nruns   = 3;              // Number of models to run

fdb5::Config theconfig(bool useSubToc = false) {

    const std::string config_str(R"XX(
        ---
        type: local
        engine: toc
        useSubToc: true
        spaces:
        - roots:
          - path: "./debug_root"
    )XX");

    eckit::testing::SetEnv env("FDB5_CONFIG", config_str.c_str());

    fdb5::Config expanded = fdb5::Config().expandConfig();
    if (!useSubToc) return expanded;
    
    eckit::LocalConfiguration userConf;
    userConf.set("useSubToc", true);

    fdb5::Config cfg(expanded, userConf);

    return cfg;
}

void runmodel(bool useSubToc = false) {
    // Write a bunch of data using 3 processes, with subtocs on. 
    eckit::Log::info() << "Start runmodel" << std::endl;
    {
        //  make fakemodel directory
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
        std::vector<int> data={1,2,3};

        std::vector<pid_t> pids(Nforks);
        for (int i = 0; i < Nforks; i++) {
            pids[i] = fork();
            ASSERT(pids[i] >= 0);
            if (pids[i] == 0) { // child
                fdb5::FDB fdb(theconfig(useSubToc));
                k.set("param", std::to_string(167+i));
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
        for (int i = 0; i < Nforks; i++) {
            int status;
            waitpid(pids[i], &status, 0);
            EXPECT(status == 0);
        }
    }
    eckit::Log::info() << "Finish runmodel" << std::endl;
}

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

        EXPECT(count == expected);
}
    eckit::Log::info() << "Finish list" << std::endl;
}

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

void cleanup(){
    // remove all contents of dir
    auto td = eckit::PathName(foldername);
    if (td.exists()) {
        std::vector<eckit::PathName> files;
        std::vector<eckit::PathName> dir;
        td.childrenRecursive(files, dir);
        for (auto f : files) {
            // Paranoia: file must have either "toc" in the prefix, or ".data" or ".index" in the suffix. Or is schema.
            std::string base = f.baseName();
            if (base.find("toc") == 0 || base.find(".data") == base.size()-5 || base.find(".index") == base.size()-6 || base.find("schema") == 0) {
                f.unlink();
            }

        }

        // and rm the dir
        for (auto d : dir) {
            d.rmdir();
        }

        eckit::PathName(foldername).rmdir(); // only removes if it is empty
    }
}


CASE( "Similar to FDB-425 but without subtocs." ) {

    // start fresh:
    cleanup();

    bool subtocs = false;
    for (int i = 0; i < Nruns; i++) {
        runmodel(subtocs); 
    }
    // Verify fdb list works pre-purge: 9 masked 9 unmasked
    list(true, Nunique);
    list(false, Nruns*Nunique);

    purge(/* doit = */ true);
    

    // Rerun the list, correct behaviour would be 0 masked 9 unmasked and dedupe should do nothing
    list(true, Nunique);
    list(false, Nunique);
}


// There are actually 2 cases we care about:
// Case 1 - We are purging all of a subtoc. In this case we really should be TOC_CLEAR on the subtoc, dont need to explicitly clear each of its indexes
// Case 2 - If we are purging only part of a sub toc, we need to TOC_CLEAR each of the relevant indexes only.
//          Case 2 is what currently happens, though it is not correctly implemented as when we enter a subtoc in the toc handler, it doesn't seem to be aware of the fact that
//          A parent toc masked the subtoc.
CASE( "Reproduce FDB-425" ) {

    // start fresh:
    cleanup();

    bool subtocs = true;
    for (int i = 0; i < Nruns; i++) {
        runmodel(subtocs); 
    }

    // Verify fdb list works pre-purge: 9 masked 9 unmasked
    list(true, Nunique);
    list(false, Nruns*Nunique);

    purge(/* doit = */ true);
    
    // Rerun the list, correct behaviour would be 0 masked 9 unmasked and dedupe should do nothing
    list(true, Nunique);
    list(false, Nunique);
}
 
 
 //----------------------------------------------------------------------------------------------------------------------
 
 }  // namespace test
 }  // namespace fdb
 
 int main(int argc, char **argv)
 {
     return run_tests ( argc, argv );
 }
 
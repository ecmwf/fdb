/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Tiago Quintino
/// @date Sep 2012

#include <cstring>
#include <memory>

#include <dirent.h>
#include <fcntl.h>

#include "eckit/io/AutoCloser.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/runtime/Main.h"
#include "eckit/serialisation/FileStream.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/types/Types.h"
#include "eckit/utils/Literals.h"
#include "eckit/utils/Translator.h"

#include "metkit/mars/MarsExpansion.h"
#include "metkit/mars/MarsRequest.h"
#include "metkit/mars/TypeAny.h"

#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/database/ArchiveVisitor.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/Key.h"

#include "eckit/testing/Test.h"

using namespace std;
using namespace eckit;
using namespace eckit::literals;
using namespace eckit::testing;
using namespace fdb5;


namespace fdb {
namespace test {

//----------------------------------------------------------------------------------------------------------------------

struct FixtureService {

    metkit::mars::MarsRequest env;
    StringDict p;
    fdb5::Config config;

    std::vector<std::string> modelParams_;

    FixtureService() : env("environ") {
        p["class"]  = "rd";
        p["stream"] = "oper";
        p["domain"] = "g";
        p["expver"] = "0001";
        p["date"]   = "20120911";
        p["time"]   = "0000";
        p["type"]   = "fc";

        modelParams_.push_back("130.128");
        modelParams_.push_back("138.128");
    }

    void write_cycle(fdb5::Archiver& fdb, StringDict& p) {
        Translator<size_t, std::string> str;
        std::vector<std::string>::iterator param = modelParams_.begin();
        for (; param != modelParams_.end(); ++param) {
            p["param"] = *param;

            p["levtype"] = "pl";

            for (size_t step = 0; step < 12; ++step) {
                p["step"] = str(step * 3);

                for (size_t level = 0; level < 10; ++level) {
                    p["levelist"] = str(level * 100);

                    std::ostringstream data;
                    data << "Raining cats and dogs -- "
                         << " param " << *param << " step " << step << " level " << level << std::endl;
                    std::string data_str = data.str();

                    fdb5::Key k{p};
                    auto visitor =
                        ArchiveVisitor::create(fdb, k, static_cast<const void*>(data_str.c_str()), data_str.size());
                    fdb.archive(k, *visitor);
                }
            }
        }
    }
};

//----------------------------------------------------------------------------------------------------------------------

CASE("test_fdb_stepunit_archive") {

    std::string data_str = "Raining cats and dogs";

    fdb5::FDB fdb;

    Key key;
    key.set("class", "od");
    key.set("expver", "0001");
    key.set("type", "fc");
    key.set("stream", "oper");
    key.set("date", "20101010");
    key.set("time", "0000");
    key.set("domain", "g");
    key.set("levtype", "sfc");
    key.set("step", "60");
    key.set("param", "130");
    fdb.archive(key, static_cast<const void*>(data_str.c_str()), data_str.size());
    fdb.flush();

    metkit::mars::MarsRequest req     = key.request();
    metkit::mars::MarsRequest listReq = key.request("list");

    {
        fdb5::FDBToolRequest r(req);
        fdb5::ListIterator iter = fdb.list(r, true);
        fdb5::ListElement el;
        EXPECT(iter.next(el));
        EXPECT(el.combinedKey().get("step") == "60");
        EXPECT(!iter.next(el));
    }

    key.set("step", "2");
    key.set("stepunits", "h");
    fdb.archive(key, static_cast<const void*>(data_str.c_str()), data_str.size());
    fdb.flush();

    req.setValue("step", "2");
    {
        fdb5::FDBToolRequest r(req);
        fdb5::ListIterator iter = fdb.list(r, true);
        fdb5::ListElement el;
        EXPECT(iter.next(el));
        EXPECT(el.combinedKey().get("step") == "2");
        EXPECT(!iter.next(el));
    }

    req.setValuesTyped(new metkit::mars::TypeAny("step"), std::vector<std::string>{"2", "60"});
    {
        fdb5::FDBToolRequest r(req);
        fdb5::ListIterator iter = fdb.list(r, true);
        fdb5::ListElement el;
        EXPECT(iter.next(el));
        EXPECT(el.combinedKey().get("step") == "2");
        EXPECT(iter.next(el));
        EXPECT(el.combinedKey().get("step") == "60");
        EXPECT(!iter.next(el));
    }

    key.set("step", "30");
    key.set("stepunits", "m");
    fdb.archive(key, static_cast<const void*>(data_str.c_str()), data_str.size());
    fdb.flush();

    req.setValue("step", "30m");
    {
        fdb5::FDBToolRequest r(req);
        fdb5::ListIterator iter = fdb.list(r, true);
        fdb5::ListElement el;
        EXPECT(iter.next(el));
        EXPECT(el.combinedKey().get("step") == "30m");
        EXPECT(!iter.next(el));
    }

    listReq.values("step", {"0", "to", "2", "by", "30m"});
    listReq.unsetValues("param");
    {
        metkit::mars::MarsExpansion expand{false};

        metkit::mars::MarsRequest expandedRequests = expand.expand(listReq);
        fdb5::FDBToolRequest r(expandedRequests);
        fdb5::ListIterator iter = fdb.list(r, true);
        fdb5::ListElement el;
        EXPECT(iter.next(el));
        EXPECT(el.combinedKey().get("step") == "30m");
        EXPECT(iter.next(el));
        EXPECT(el.combinedKey().get("step") == "2");
        EXPECT(!iter.next(el));
    }
}

CASE("test_fdb_service") {

    SETUP("Fixture") {
        FixtureService f;

        SECTION("test_fdb_service_write") {
            fdb5::Archiver fdb;

            f.p["class"]  = "rd";
            f.p["stream"] = "oper";
            f.p["domain"] = "g";
            f.p["expver"] = "0001";

            f.p["date"] = "20120911";
            f.p["time"] = "0000";
            f.p["type"] = "fc";

            f.write_cycle(fdb, f.p);

            f.p["date"] = "20120911";
            f.p["time"] = "0000";
            f.p["type"] = "4v";

            f.write_cycle(fdb, f.p);

            f.p["date"] = "20120912";
            f.p["time"] = "0000";
            f.p["type"] = "fc";

            f.write_cycle(fdb, f.p);

            f.p["date"] = "20120912";
            f.p["time"] = "0000";
            f.p["type"] = "4v";

            f.write_cycle(fdb, f.p);
        }


        SECTION("test_fdb_service_readtobuffer") {
            fdb5::FDB retriever;

            Buffer buffer(1_KiB);

            Translator<size_t, std::string> str;
            std::vector<std::string>::iterator param = f.modelParams_.begin();
            for (; param != f.modelParams_.end(); ++param) {
                f.p["param"]   = *param;
                f.p["levtype"] = "pl";

                for (size_t step = 0; step < 2; ++step) {
                    f.p["step"] = str(step * 3);

                    for (size_t level = 0; level < 3; ++level) {
                        f.p["levelist"] = str(level * 100);

                        Log::info() << "Looking for: " << f.p << std::endl;

                        metkit::mars::MarsRequest r("retrieve", f.p);
                        std::unique_ptr<DataHandle> dh(retriever.retrieve(r));
                        AutoClose closer1(*dh);

                        ::memset(buffer, 0, buffer.size());

                        dh->openForRead();
                        dh->read(buffer, buffer.size());

                        Log::info() << (char*)buffer << std::endl;

                        std::ostringstream data;
                        data << "Raining cats and dogs -- "
                             << " param " << *param << " step " << step << " level " << level << std::endl;

                        EXPECT(::memcmp(buffer, data.str().c_str(), data.str().size()) == 0);
                    }
                }
            }
        }


        SECTION("test_fdb_service_list") {
            fdb5::FDB lister;

            Translator<size_t, std::string> str;
            std::vector<std::string>::iterator param = f.modelParams_.begin();
            for (; param != f.modelParams_.end(); ++param) {
                f.p["param"]    = *param;
                f.p["levtype"]  = "pl";
                f.p["step"]     = str(0);
                f.p["levelist"] = str(0);

                Log::info() << "Looking for: " << f.p << std::endl;

                metkit::mars::MarsRequest r("retrieve", f.p);
                fdb5::FDBToolRequest req(r);
                auto iter = lister.list(req);
                fdb5::ListElement el;
                EXPECT(iter.next(el));

                eckit::PathName path = el.uri().path().dirName();

                DIR* dirp = ::opendir(path.asString().c_str());
                struct dirent* dp;
                while ((dp = ::readdir(dirp)) != nullptr) {
                    EXPECT_NOT(strstr(dp->d_name, "toc."));
                }
                ::closedir(dirp);

                // consuming the rest of the queue
                while (iter.next(el))
                    ;
            }
        }

        SECTION("test_fdb_service_marsreques") {
            std::vector<string> steps;
            steps.push_back("15");
            steps.push_back("18");
            steps.push_back("24");

            std::vector<string> levels;
            levels.push_back("100");
            levels.push_back("500");

            std::vector<string> params;
            params.push_back("130.128");
            params.push_back("138.128");

            std::vector<string> dates;
            dates.push_back("20120911");
            dates.push_back("20120912");


            metkit::mars::MarsRequest r("retrieve");

            r.setValue("class", "rd");
            r.setValue("expver", "0001");
            r.setValue("type", "fc");
            r.setValue("stream", "oper");
            r.setValue("time", "0000");
            r.setValue("domain", "g");
            r.setValue("levtype", "pl");

            r.setValuesTyped(new metkit::mars::TypeAny("param"), params);
            r.setValuesTyped(new metkit::mars::TypeAny("step"), steps);
            r.setValuesTyped(new metkit::mars::TypeAny("levelist"), levels);
            r.setValuesTyped(new metkit::mars::TypeAny("date"), dates);

            Log::info() << r << std::endl;

            fdb5::FDB retriever;

            std::unique_ptr<DataHandle> dh(retriever.retrieve(r));

            PathName path("data_mars_request.data");
            path.unlink();

            dh->saveInto(path);
        }
    }
}


CASE("test_fdb_service_subtoc") {

    SETUP("Fixture") {
        FixtureService f;

        eckit::LocalConfiguration userConf;
        userConf.set("useSubToc", true);

        fdb5::Config expanded = fdb5::Config().expandConfig();

        fdb5::Config config(expanded, userConf);

        SECTION("test_fdb_service_subtoc_write") {
            fdb5::Archiver fdb(config);

            f.p["class"]  = "rd";
            f.p["stream"] = "oper";
            f.p["domain"] = "g";
            f.p["expver"] = "0002";

            f.p["date"] = "20120911";
            f.p["time"] = "0000";
            f.p["type"] = "fc";

            f.write_cycle(fdb, f.p);

            f.p["date"] = "20120911";
            f.p["time"] = "0000";
            f.p["type"] = "4v";

            f.write_cycle(fdb, f.p);

            f.p["date"] = "20120912";
            f.p["time"] = "0000";
            f.p["type"] = "fc";

            f.write_cycle(fdb, f.p);

            f.p["date"] = "20120912";
            f.p["time"] = "0000";
            f.p["type"] = "4v";

            f.write_cycle(fdb, f.p);
        }

        SECTION("test_fdb_service_subtoc_readtobuffer") {
            fdb5::FDB retriever;

            Buffer buffer(1_KiB);

            f.p["expver"] = "0002";

            Translator<size_t, std::string> str;
            std::vector<std::string>::iterator param = f.modelParams_.begin();
            for (; param != f.modelParams_.end(); ++param) {
                f.p["param"]   = *param;
                f.p["levtype"] = "pl";

                for (size_t step = 0; step < 2; ++step) {
                    f.p["step"] = str(step * 3);

                    for (size_t level = 0; level < 3; ++level) {
                        f.p["levelist"] = str(level * 100);

                        Log::info() << "Looking for: " << f.p << std::endl;

                        metkit::mars::MarsRequest r("retrieve", f.p);
                        std::unique_ptr<DataHandle> dh(retriever.retrieve(r));
                        AutoClose closer1(*dh);

                        ::memset(buffer, 0, buffer.size());

                        dh->openForRead();
                        dh->read(buffer, buffer.size());

                        Log::info() << (char*)buffer << std::endl;

                        std::ostringstream data;
                        data << "Raining cats and dogs -- "
                             << " param " << *param << " step " << step << " level " << level << std::endl;

                        EXPECT(::memcmp(buffer, data.str().c_str(), data.str().size()) == 0);
                    }
                }
            }
        }


        SECTION("test_fdb_service_subtoc_list") {
            fdb5::FDB lister;

            f.p["expver"] = "0002";

            Translator<size_t, std::string> str;
            std::vector<std::string>::iterator param = f.modelParams_.begin();
            for (; param != f.modelParams_.end(); ++param) {

                f.p["param"]    = *param;
                f.p["levtype"]  = "pl";
                f.p["step"]     = str(0);
                f.p["levelist"] = str(0);

                Log::info() << "Looking for: " << f.p << std::endl;

                metkit::mars::MarsRequest r("retrieve", f.p);
                fdb5::FDBToolRequest req(r);
                auto iter = lister.list(req);
                fdb5::ListElement el;
                EXPECT(iter.next(el));

                eckit::PathName path = el.uri().path().dirName();

                DIR* dirp = ::opendir(path.asString().c_str());
                struct dirent* dp;
                bool subtoc = false;
                while ((dp = ::readdir(dirp)) != nullptr) {
                    if (strstr(dp->d_name, "toc.")) {
                        subtoc = true;
                    }
                }
                EXPECT(subtoc);
                ::closedir(dirp);

                // consuming the rest of the queue
                while (iter.next(el))
                    ;
            }
        }

        SECTION("test_fdb_service_subtoc_marsreques") {
            std::vector<string> steps;
            steps.push_back("15");
            steps.push_back("18");
            steps.push_back("24");

            std::vector<string> levels;
            levels.push_back("100");
            levels.push_back("500");

            std::vector<string> params;
            params.push_back("130.128");
            params.push_back("138.128");

            std::vector<string> dates;
            dates.push_back("20120911");
            dates.push_back("20120912");


            metkit::mars::MarsRequest r("retrieve");

            r.setValue("class", "rd");
            r.setValue("expver", "0002");
            r.setValue("type", "fc");
            r.setValue("stream", "oper");
            r.setValue("time", "0000");
            r.setValue("domain", "g");
            r.setValue("levtype", "pl");

            r.setValuesTyped(new metkit::mars::TypeAny("param"), params);
            r.setValuesTyped(new metkit::mars::TypeAny("step"), steps);
            r.setValuesTyped(new metkit::mars::TypeAny("levelist"), levels);
            r.setValuesTyped(new metkit::mars::TypeAny("date"), dates);

            Log::info() << r << std::endl;

            fdb5::FDB retriever;

            std::unique_ptr<DataHandle> dh(retriever.retrieve(r));

            PathName path("data_mars_request.data");
            path.unlink();

            dh->saveInto(path);
        }
    }
}

CASE("schemaSerialisation") {

    PathName filename    = PathName::unique("data");
    std::string filepath = filename.asString();

    std::string original;

    {
        eckit::FileStream sout(filepath.c_str(), "w");
        auto c = eckit::closer(sout);

        fdb5::Config config;
        config = config.expandConfig();

        std::ostringstream ss;
        config.schema().dump(ss);
        original = ss.str();

        sout << config.schema();
    }
    {
        eckit::FileStream sin(filepath.c_str(), "r");
        auto c = eckit::closer(sin);

        std::unique_ptr<fdb5::Schema> clone(eckit::Reanimator<fdb5::Schema>::reanimate(sin));

        std::ostringstream ss;
        clone->dump(ss);

        EXPECT(original == ss.str());
    }
    if (filename.exists()) {
        filename.unlink();
    }
}

//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb

int main(int argc, char** argv) {
    eckit::Main::initialise(argc, argv, "FDB_HOME");
    return run_tests(argc, argv, false);
}

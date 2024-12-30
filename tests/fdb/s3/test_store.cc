/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/YAMLConfiguration.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/s3/S3BucketName.h"
#include "eckit/io/s3/S3Handle.h"
#include "eckit/io/s3/S3Session.h"
#include "eckit/testing/Test.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/api/helpers/ListIterator.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/config/Config.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/database/WipeVisitor.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/s3/S3Store.h"
#include "fdb5/toc/TocCatalogueReader.h"
#include "fdb5/toc/TocCatalogueWriter.h"
#include "test_s3_config.h"

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

using namespace eckit::testing;
using namespace eckit;

namespace fdb::test {

//----------------------------------------------------------------------------------------------------------------------

namespace {

const std::vector<std::string> testBuckets {S3_TEST_BUCKET};

void deldir(const eckit::PathName& root) {

    if (!root.exists()) { return; }

    std::vector<eckit::PathName> files;
    std::vector<eckit::PathName> dirs;
    root.children(files, dirs);

    for (auto& f : files) { f.unlink(); }
    for (auto& d : dirs) { deldir(d); }

    root.rmdir();
};

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

CASE("Setup") {

    // create root directory for tests

    const eckit::PathName rootDir {S3_TEST_ROOT};
    deldir(rootDir);
    EXPECT_NO_THROW(rootDir.mkdir());

    EXPECT_NO_THROW(S3Session::instance().addClient(s3::TEST_CONFIG));
    EXPECT_NO_THROW(S3Session::instance().addCredential(s3::TEST_CRED));
}

CASE("S3Store tests") {

    s3::cleanup(testBuckets);
    eckit::S3BucketName(s3::TEST_ENDPOINT, {S3_TEST_BUCKET}).create();

    SECTION("archive and retrieve") {

        auto config = fdb5::Config::make("./config.yaml");

        fdb5::Key requestKey;
        requestKey.set("a", "1");
        requestKey.set("b", "2");
        requestKey.set("c", "3");
        requestKey.set("d", "4");
        requestKey.set("e", "5");
        requestKey.set("f", "6");

        fdb5::Key dbKey;
        dbKey.set("a", "1");
        dbKey.set("b", "2");

        fdb5::Key indexKey;
        indexKey.set("c", "3");
        indexKey.set("d", "4");

        char data[] = "test";

        // archive

        fdb5::S3Store s3store {config.schema(), dbKey, config};
        fdb5::Store&  store = s3store;
        auto          loc   = store.archive(indexKey, data, sizeof(data));

        s3store.flush();

        // retrieve
        fdb5::Field field(std::move(loc), std::time(nullptr));
        std::cout << "Read location: " << field.location() << std::endl;
        std::unique_ptr<eckit::DataHandle> dh(store.retrieve(field));
        EXPECT(dynamic_cast<eckit::S3Handle*>(dh.get()));

        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);

        // remove
        const URI           fieldURI = field.location().uri();
        eckit::S3ObjectName field_name {fieldURI};
        eckit::S3BucketName store_name {fieldURI, field_name.path().bucket};
        eckit::URI          store_uri(store_name.uri());
        std::ostream        out(std::cout.rdbuf());
        store.remove(store_uri, out, out, false);
        EXPECT(field_name.exists());
        store.remove(store_uri, out, out, true);
        EXPECT_NOT(field_name.exists());
        EXPECT_NOT(store_name.exists());
    }

    SECTION("with POSIX Catalogue") {

        auto config = fdb5::Config::make("./config.yaml");

        // request

        fdb5::Key requestKey;
        requestKey.set("a", "1");
        requestKey.set("b", "2");
        requestKey.set("c", "3");
        requestKey.set("d", "4");
        requestKey.set("e", "5");
        requestKey.set("f", "6");

        fdb5::Key dbKey;
        dbKey.set("a", "1");
        dbKey.set("b", "2");

        fdb5::Key indexKey;
        indexKey.set("c", "3");
        indexKey.set("d", "4");

        fdb5::Key fieldKey;
        fieldKey.set("e", "5");
        fieldKey.set("f", "6");

        // store data

        char data[] = "test";

        fdb5::S3Store s3store {config.schema(), dbKey, config};

        auto& store = static_cast<fdb5::Store&>(s3store);
        auto  loc   = store.archive(indexKey, data, sizeof(data));

        // index data

        {
            fdb5::TocCatalogueWriter tcat {dbKey, config};

            auto& cat = static_cast<fdb5::Catalogue&>(tcat);
            cat.deselectIndex();
            cat.selectIndex(indexKey);
            // const fdb5::Index& idx = tcat.currentIndex();
            static_cast<fdb5::CatalogueWriter&>(tcat).archive(fieldKey, std::move(loc));

            /// flush store before flushing catalogue
            s3store.flush();  // not necessary if using a DAOS store
        }

        // find data

        fdb5::Field field;
        {
            fdb5::TocCatalogueReader tcat {dbKey, config};
            fdb5::Catalogue&         cat = static_cast<fdb5::Catalogue&>(tcat);
            cat.selectIndex(indexKey);
            static_cast<fdb5::CatalogueReader&>(tcat).retrieve(fieldKey, field);
        }
        std::cout << "Read location: " << field.location() << std::endl;

        // retrieve data

        std::unique_ptr<eckit::DataHandle> dh(store.retrieve(field));
        EXPECT(dynamic_cast<eckit::S3Handle*>(dh.get()));

        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);

        // remove data

        const URI           fieldURI = field.location().uri();
        eckit::S3ObjectName field_name {fieldURI};
        eckit::S3BucketName store_name {fieldURI, field_name.path().bucket};
        eckit::URI          store_uri(store_name.uri());
        std::ostream        out(std::cout.rdbuf());
        store.remove(store_uri, out, out, false);
        EXPECT(field_name.exists());
        store.remove(store_uri, out, out, true);
        EXPECT_NOT(field_name.exists());
        EXPECT_NOT(store_name.exists());

        // deindex data

        {
            fdb5::TocCatalogueWriter           tcat {dbKey, config};
            fdb5::Catalogue&                   cat = static_cast<fdb5::Catalogue&>(tcat);
            metkit::mars::MarsRequest          r   = dbKey.request("retrieve");
            std::unique_ptr<fdb5::WipeVisitor> wv(cat.wipeVisitor(store, r, out, true, false, false));
            cat.visitEntries(*wv, store, false);
        }
    }

    SECTION("VIA FDB API") {

        auto config = fdb5::Config::make("./config.yaml");

        // request

        fdb5::Key requestKey;
        requestKey.set("a", "1");
        requestKey.set("b", "2");
        requestKey.set("c", "3");
        requestKey.set("d", "4");
        requestKey.set("e", "5");
        requestKey.set("f", "6");

        fdb5::Key dbKey;
        dbKey.set("a", "1");
        dbKey.set("b", "2");

        fdb5::Key indexKey;
        indexKey.set("a", "1");
        indexKey.set("b", "2");
        indexKey.set("c", "3");
        indexKey.set("d", "4");

        fdb5::FDBToolRequest fullReq {requestKey.request("retrieve"), false, std::vector<std::string> {"a", "b"}};
        fdb5::FDBToolRequest indexReq {indexKey.request("retrieve"), false, std::vector<std::string> {"a", "b"}};
        fdb5::FDBToolRequest dbReq {dbKey.request("retrieve"), false, std::vector<std::string> {"a", "b"}};

        // initialise store

        fdb5::FDB fdb(config);

        // check store is empty

        size_t count = 0;

        fdb5::ListElement info;

        auto listObject = fdb.list(dbReq);

        while (listObject.next(info)) {
            info.print(std::cout, true, true);
            std::cout << std::endl;
            ++count;
        }
        EXPECT(count == 0);

        // store data

        char data[] = "test";

        fdb.archive(requestKey, data, sizeof(data));

        fdb.flush();

        // retrieve data

        metkit::mars::MarsRequest          r = requestKey.request("retrieve");
        std::unique_ptr<eckit::DataHandle> dh(fdb.retrieve(r));

        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);

        // wipe data

        fdb5::WipeElement elem;

        // dry run attempt to wipe with too specific request

        auto wipeObject = fdb.wipe(fullReq);
        count           = 0;
        while (wipeObject.next(elem)) { count++; }
        EXPECT(count == 0);

        // dry run wipe index and store unit
        wipeObject = fdb.wipe(indexReq);
        count      = 0;
        while (wipeObject.next(elem)) { count++; }
        EXPECT(count > 0);

        // dry run wipe database
        wipeObject = fdb.wipe(dbReq);
        count      = 0;
        while (wipeObject.next(elem)) { count++; }
        EXPECT(count > 0);

        // ensure field still exists
        listObject = fdb.list(fullReq);
        count      = 0;
        while (listObject.next(info)) {
            // info.print(std::cout, true, true);
            // std::cout << std::endl;
            count++;
        }
        EXPECT(count == 1);

        // attempt to wipe with too specific request
        wipeObject = fdb.wipe(fullReq, true);
        count      = 0;
        while (wipeObject.next(elem)) { count++; }
        EXPECT(count == 0);
        /// @todo: really needed?
        fdb.flush();

        // wipe index and store unit (and DB bucket as there is only one index)
        wipeObject = fdb.wipe(indexReq, true);
        count      = 0;
        while (wipeObject.next(elem)) { count++; }
        EXPECT(count > 0);
        /// @todo: really needed?
        fdb.flush();

        // ensure field does not exist
        listObject = fdb.list(fullReq);
        count      = 0;
        while (listObject.next(info)) { count++; }
        EXPECT(count == 0);
    }

    /// @todo: if doing what's in this section at the end of the previous section reusing the same FDB object,
    // archive() fails as it expects a toc file to exist, but it has been removed by previous wipe
    SECTION("FDB API RE-STORE AND WIPE DB") {

        auto config = fdb5::Config::make("./config.yaml");

        // request

        fdb5::Key requestKey;
        requestKey.set("a", "1");
        requestKey.set("b", "2");
        requestKey.set("c", "3");
        requestKey.set("d", "4");
        requestKey.set("e", "5");
        requestKey.set("f", "6");

        fdb5::Key dbKey;
        dbKey.set("a", "1");
        dbKey.set("b", "2");

        fdb5::Key indexKey;
        indexKey.set("a", "1");
        indexKey.set("b", "2");
        indexKey.set("c", "3");
        indexKey.set("d", "4");

        fdb5::FDBToolRequest fullReq {requestKey.request("retrieve"), false, std::vector<std::string> {"a", "b"}};
        fdb5::FDBToolRequest indexReq {indexKey.request("retrieve"), false, std::vector<std::string> {"a", "b"}};
        fdb5::FDBToolRequest dbReq {dbKey.request("retrieve"), false, std::vector<std::string> {"a", "b"}};

        // initialise store

        fdb5::FDB fdb(config);

        // store again

        char data[] = "test";

        fdb.archive(requestKey, data, sizeof(data));

        fdb.flush();

        // wipe all database

        size_t count = 0;

        fdb5::WipeElement elem;

        auto wipeObject = fdb.wipe(dbReq, true);
        while (wipeObject.next(elem)) { count++; }

        EXPECT(count > 0);

        // ensure field does not exist

        count = 0;

        fdb5::ListElement info;

        auto listObject = fdb.list(fullReq);
        while (listObject.next(info)) {
            // info.print(std::cout, true, true);
            // std::cout << std::endl;
            count++;
        }

        EXPECT(count == 0);
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}

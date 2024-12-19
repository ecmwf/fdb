/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include <cstring>
// #include <memory>

// #include "eckit/config/Resource.h"
#include "eckit/testing/Test.h"
// #include "eckit/filesystem/URI.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpFile.h"
// #include "eckit/filesystem/TmpDir.h"
// #include "eckit/io/FileHandle.h"
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/io/MemoryHandle.h"

// #include "metkit/mars/MarsRequest.h"

// #include "fdb5/fdb5_config.h"
// #include "fdb5/config/Config.h"
#include "eckit/io/s3/S3BucketName.h"
#include "eckit/io/s3/S3Client.h"
#include "eckit/io/s3/S3Credential.h"
#include "eckit/io/s3/S3Handle.h"
#include "eckit/io/s3/S3Session.h"
#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/database/Key.h"
#include "fdb5/s3/S3FieldLocation.h"
#include "fdb5/s3/S3Store.h"
#include "fdb5/toc/TocCatalogueReader.h"
#include "fdb5/toc/TocCatalogueWriter.h"
// #include "fdb5/daos/DaosException.h"

using namespace eckit::testing;
using namespace eckit;

namespace {

void deldir(eckit::PathName& p) {
    if (!p.exists()) { return; }

    std::vector<eckit::PathName> files;
    std::vector<eckit::PathName> dirs;
    p.children(files, dirs);

    for (auto& f : files) { f.unlink(); }
    for (auto& d : dirs) { deldir(d); }

    p.rmdir();
};

S3Config cfg("minio", 9000, "eu-central-1");

void ensureClean(const std::string& prefix) {
    const eckit::S3Credential cred {{"minio:9000"}, "minio", "minio1234"};
    eckit::S3Session::instance().addCredential(cred);
    auto                  client = S3Client::makeUnique(cfg);
    auto&&                tmp    = client->listBuckets();
    std::set<std::string> buckets(tmp.begin(), tmp.end());

    for (const std::string& name : buckets) {
        if (name.rfind(prefix, 0) == 0) {
            client->emptyBucket(name);
            client->deleteBucket(name);
        }
    }
    eckit::S3Session::instance().clear();
}
}  // namespace

// #ifdef fdb5_HAVE_DUMMY_DAOS
// eckit::TmpDir& tmp_dummy_daos_root() {
//     static eckit::TmpDir d{};
//     return d;
// }
// #endif

// temporary schema,spaces,root files common to all DAOS Store tests

eckit::TmpFile& schema_file() {
    static eckit::TmpFile f {};
    return f;
}

eckit::TmpFile& spaces_file() {
    static eckit::TmpFile f {};
    return f;
}

eckit::TmpFile& roots_file() {
    static eckit::TmpFile f {};
    return f;
}

eckit::PathName& store_tests_tmp_root() {
    static eckit::PathName sd("./s3_store_tests_fdb_root");
    return sd;
}

namespace fdb { namespace test {

CASE("Setup") {

    // ensure fdb root directory exists. If not, then that root is
    // registered as non existing and Store tests fail.
    if (store_tests_tmp_root().exists()) { deldir(store_tests_tmp_root()); }
    store_tests_tmp_root().mkdir();
    ::setenv("FDB_ROOT_DIRECTORY", store_tests_tmp_root().path().c_str(), 1);

    // prepare schema for tests involving S3Store

    std::string schema_str {"[ a, b [ c, d [ e, f ]]]"};

    std::unique_ptr<eckit::DataHandle> hs(schema_file().fileHandle());
    hs->openForWrite(schema_str.size());
    {
        eckit::AutoClose closer(*hs);
        hs->write(schema_str.data(), schema_str.size());
    }

    // this is necessary to avoid ~fdb/etc/fdb/schema being used where
    // LibFdb5::instance().defaultConfig().schema() is called
    // due to no specified schema file (e.g. in Key::registry())
    ::setenv("FDB_SCHEMA_FILE", schema_file().path().c_str(), 1);

    // prepare scpaces

    std::string spaces_str {".* all Default"};

    std::unique_ptr<eckit::DataHandle> hsp(spaces_file().fileHandle());
    hsp->openForWrite(spaces_str.size());
    {
        eckit::AutoClose closer(*hsp);
        hsp->write(spaces_str.data(), spaces_str.size());
    }

    ::setenv("FDB_SPACES_FILE", spaces_file().path().c_str(), 1);

    // prepare roots

    std::string roots_str {store_tests_tmp_root().asString() + " all yes yes"};

    std::unique_ptr<eckit::DataHandle> hr(roots_file().fileHandle());
    hr->openForWrite(roots_str.size());
    {
        eckit::AutoClose closer(*hr);
        hr->write(roots_str.data(), roots_str.size());
    }

    ::setenv("FDB_ROOTS_FILE", roots_file().path().c_str(), 1);
}

CASE("S3Store tests") {

    SECTION("archive and retrieve") {

        std::string prefix {"test1-"};

        ensureClean(prefix);

        std::string
            config_str {
                "s3:\n  credential: ./S3Credentials.yaml\n  endpoint: minio:9000\n  " "region: " "\"eu-central-1\"\n  " "bucketPrefix: " +
                prefix + "\n"};

        fdb5::Config config {YAMLConfiguration(config_str)};

        fdb5::Schema schema {schema_file()};

        fdb5::Key request_key;
        request_key.set("a", "1");
        request_key.set("b", "2");
        request_key.set("c", "3");
        request_key.set("d", "4");
        request_key.set("e", "5");
        request_key.set("f", "6");

        fdb5::Key db_key;
        db_key.set("a", "1");
        db_key.set("b", "2");

        fdb5::Key index_key;
        index_key.set("c", "3");
        index_key.set("d", "4");

        char data[] = "test";

        // archive

        fdb5::S3Store s3store {schema, db_key, config};
        fdb5::Store&  store = s3store;
        auto          loc   = store.archive(index_key, data, sizeof(data));

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
        eckit::S3BucketName store_name {fieldURI, field_name.bucket()};
        eckit::URI          store_uri(store_name.uri());
        std::ostream        out(std::cout.rdbuf());
        store.remove(store_uri, out, out, false);
        EXPECT(field_name.exists());
        store.remove(store_uri, out, out, true);
        EXPECT_NOT(field_name.exists());
        EXPECT_NOT(store_name.exists());
    }

    SECTION("with POSIX Catalogue") {

        std::string prefix {"test2-"};

        ensureClean(prefix);

        // FDB configuration

        std::string config_str {
            "schema : " +
            schema_file().path() + "\ns3:\n  credential: ./S3Credentials.yaml\n  endpoint: minio:9000\n  " "region: \"eu-central-1\"\n  bucketPrefix: " +
            prefix + "\n"};

        fdb5::Config config {YAMLConfiguration(config_str)};

        // schema

        fdb5::Schema schema {schema_file()};

        // request

        fdb5::Key request_key;
        request_key.set("a", "1");
        request_key.set("b", "2");
        request_key.set("c", "3");
        request_key.set("d", "4");
        request_key.set("e", "5");
        request_key.set("f", "6");

        fdb5::Key db_key;
        db_key.set("a", "1");
        db_key.set("b", "2");

        fdb5::Key index_key;
        index_key.set("c", "3");
        index_key.set("d", "4");

        fdb5::Key field_key;
        field_key.set("e", "5");
        field_key.set("f", "6");

        // store data

        char data[] = "test";

        fdb5::S3Store s3store {schema, db_key, config};
        fdb5::Store&  store = static_cast<fdb5::Store&>(s3store);
        auto          loc   = store.archive(index_key, data, sizeof(data));

        // index data

        {
            /// @todo: could have a unique ptr here, might not need a static cast
            fdb5::TocCatalogueWriter tcat {db_key, config};
            fdb5::Catalogue&         cat = static_cast<fdb5::Catalogue&>(tcat);
            cat.deselectIndex();
            cat.selectIndex(index_key);
            // const fdb5::Index& idx = tcat.currentIndex();
            static_cast<fdb5::CatalogueWriter&>(tcat).archive(field_key, std::move(loc));

            /// flush store before flushing catalogue
            s3store.flush();  // not necessary if using a DAOS store
        }

        // find data

        fdb5::Field field;
        {
            fdb5::TocCatalogueReader tcat {db_key, config};
            fdb5::Catalogue&         cat = static_cast<fdb5::Catalogue&>(tcat);
            cat.selectIndex(index_key);
            static_cast<fdb5::CatalogueReader&>(tcat).retrieve(field_key, field);
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
        eckit::S3BucketName store_name {fieldURI, field_name.bucket()};
        eckit::URI          store_uri(store_name.uri());
        std::ostream        out(std::cout.rdbuf());
        store.remove(store_uri, out, out, false);
        EXPECT(field_name.exists());
        store.remove(store_uri, out, out, true);
        EXPECT_NOT(field_name.exists());
        EXPECT_NOT(store_name.exists());

        // deindex data

        {
            fdb5::TocCatalogueWriter           tcat {db_key, config};
            fdb5::Catalogue&                   cat = static_cast<fdb5::Catalogue&>(tcat);
            metkit::mars::MarsRequest          r   = db_key.request("retrieve");
            std::unique_ptr<fdb5::WipeVisitor> wv(cat.wipeVisitor(store, r, out, true, false, false));
            cat.visitEntries(*wv, store, false);
        }
    }

    SECTION("VIA FDB API") {

        std::string prefix {"test3-"};

        ensureClean(prefix);

        // FDB configuration

        std::string
            config_str {
                "type: local\n" "schema : " + schema_file().path() +
                "\n" "engine: toc\n" "store: s3\ns3:\n  credential: ./S3Credentials.yaml\n  endpoint: minio:9000\n  " "region: \"eu-central-1\"\n  bucketPrefix: " +
                prefix + "\n"};

        fdb5::Config config {YAMLConfiguration(config_str)};

        // request

        fdb5::Key request_key;
        request_key.set("a", "1");
        request_key.set("b", "2");
        request_key.set("c", "3");
        request_key.set("d", "4");
        request_key.set("e", "5");
        request_key.set("f", "6");

        fdb5::Key db_key;
        db_key.set("a", "1");
        db_key.set("b", "2");

        fdb5::Key index_key;
        index_key.set("a", "1");
        index_key.set("b", "2");
        index_key.set("c", "3");
        index_key.set("d", "4");

        fdb5::FDBToolRequest full_req {request_key.request("retrieve"), false, std::vector<std::string> {"a", "b"}};
        fdb5::FDBToolRequest index_req {index_key.request("retrieve"), false, std::vector<std::string> {"a", "b"}};
        fdb5::FDBToolRequest db_req {db_key.request("retrieve"), false, std::vector<std::string> {"a", "b"}};

        // initialise store

        fdb5::FDB fdb(config);

        // check store is empty

        size_t            count;
        fdb5::ListElement info;

        auto listObject = fdb.list(db_req);

        count = 0;
        while (listObject.next(info)) {
            info.print(std::cout, true, true);
            std::cout << std::endl;
            ++count;
        }
        EXPECT(count == 0);

        // store data

        char data[] = "test";

        fdb.archive(request_key, data, sizeof(data));

        fdb.flush();

        // retrieve data

        metkit::mars::MarsRequest          r = request_key.request("retrieve");
        std::unique_ptr<eckit::DataHandle> dh(fdb.retrieve(r));

        eckit::MemoryHandle mh;
        dh->copyTo(mh);
        EXPECT(mh.size() == eckit::Length(sizeof(data)));
        EXPECT(::memcmp(mh.data(), data, sizeof(data)) == 0);

        // wipe data

        fdb5::WipeElement elem;

        // dry run attempt to wipe with too specific request

        auto wipeObject = fdb.wipe(full_req);
        count           = 0;
        while (wipeObject.next(elem)) { count++; }
        EXPECT(count == 0);

        // dry run wipe index and store unit
        wipeObject = fdb.wipe(index_req);
        count      = 0;
        while (wipeObject.next(elem)) { count++; }
        EXPECT(count > 0);

        // dry run wipe database
        wipeObject = fdb.wipe(db_req);
        count      = 0;
        while (wipeObject.next(elem)) { count++; }
        EXPECT(count > 0);

        // ensure field still exists
        listObject = fdb.list(full_req);
        count      = 0;
        while (listObject.next(info)) {
            // info.print(std::cout, true, true);
            // std::cout << std::endl;
            count++;
        }
        EXPECT(count == 1);

        // attempt to wipe with too specific request
        wipeObject = fdb.wipe(full_req, true);
        count      = 0;
        while (wipeObject.next(elem)) { count++; }
        EXPECT(count == 0);
        /// @todo: really needed?
        fdb.flush();

        // wipe index and store unit (and DB bucket as there is only one index)
        wipeObject = fdb.wipe(index_req, true);
        count      = 0;
        while (wipeObject.next(elem)) { count++; }
        EXPECT(count > 0);
        /// @todo: really needed?
        fdb.flush();

        // ensure field does not exist
        listObject = fdb.list(full_req);
        count      = 0;
        while (listObject.next(info)) { count++; }
        EXPECT(count == 0);
    }

    /// @todo: if doing what's in this section at the end of the previous section reusing the same FDB object,
    // archive() fails as it expects a toc file to exist, but it has been removed by previous wipe
    SECTION("FDB API RE-STORE AND WIPE DB") {

        std::string prefix {"test4-"};

        ensureClean(prefix);

        // FDB configuration

        std::string
            config_str {
                "type: local\n" "schema : " + schema_file().path() +
                "\n" "engine: toc\n" "store: s3\ns3:\n  credential: ./S3Credentials.yaml\n  endpoint: minio:9000\n  " "region: \"eu-central-1\"\n  bucketPrefix: " +
                prefix + "\n"};

        fdb5::Config config {YAMLConfiguration(config_str)};

        // request

        fdb5::Key request_key;
        request_key.set("a", "1");
        request_key.set("b", "2");
        request_key.set("c", "3");
        request_key.set("d", "4");
        request_key.set("e", "5");
        request_key.set("f", "6");

        fdb5::Key db_key;
        db_key.set("a", "1");
        db_key.set("b", "2");

        fdb5::Key index_key;
        index_key.set("a", "1");
        index_key.set("b", "2");
        index_key.set("c", "3");
        index_key.set("d", "4");

        fdb5::FDBToolRequest full_req {request_key.request("retrieve"), false, std::vector<std::string> {"a", "b"}};
        fdb5::FDBToolRequest index_req {index_key.request("retrieve"), false, std::vector<std::string> {"a", "b"}};
        fdb5::FDBToolRequest db_req {db_key.request("retrieve"), false, std::vector<std::string> {"a", "b"}};

        // initialise store

        fdb5::FDB fdb(config);

        // store again

        char data[] = "test";

        fdb.archive(request_key, data, sizeof(data));

        fdb.flush();

        // wipe all database

        size_t count = 0;

        fdb5::WipeElement elem;

        auto wipeObject = fdb.wipe(db_req, true);
        while (wipeObject.next(elem)) { count++; }

        EXPECT(count > 0);

        // ensure field does not exist

        count = 0;

        fdb5::ListElement info;

        auto listObject = fdb.list(full_req);
        while (listObject.next(info)) {
            // info.print(std::cout, true, true);
            // std::cout << std::endl;
            count++;
        }

        EXPECT(count == 0);
    }
}

}}  // namespace fdb::test

int main(int argc, char** argv) {
    return run_tests(argc, argv);

    ensureClean("");
}

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <unistd.h>

#include "eckit/runtime/Main.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/StaticMutex.h"
#include "eckit/log/TimeStamp.h"
#include "eckit/utils/MD5.h"
#include "eckit/utils/Tokenizer.h"
#include "eckit/io/s3/S3BucketName.h"

// #include "eckit/config/Resource.h"

#include "fdb5/s3/S3FieldLocation.h"
#include "fdb5/s3/S3Store.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static StoreBuilder<S3Store> builder("s3");

S3Store::S3Store(const Schema& schema, const Key& key, const Config& config) :
    Store(schema), S3Common(config, "store", key), config_(config) {

}

S3Store::S3Store(const Schema& schema, const eckit::URI& uri, const Config& config) :
    Store(schema), S3Common(config, "store", uri), config_(config) {

}

eckit::URI S3Store::uri() const {

    return eckit::S3BucketName(endpoint_, db_bucket_).uri();


/// @note: code for single bucket for all DBs
// TODO
// // warning! here an incomplete uri is being returned. Where is this method 
// // being called? Can that caller code accept incomplete uris?
//     return eckit::S3Name(endpoint_, bucket_, db_prefix_).URI();
    
}

bool S3Store::uriBelongs(const eckit::URI& uri) const {

    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    const auto n = parts.size();


    /// @note: code for bucket per DB
    ASSERT(n == 1 || n == 2);
    return (
        (uri.scheme() == type()) && 
        (parts[0] == db_bucket_));


    /// @note: code for single bucket for all DBs
    // ASSERT(n == 2);
    // return (
    //     (uri.scheme() == type()) && 
    //     (parts[1].rfind(db_prefix_, 0) == 0));

}

bool S3Store::uriExists(const eckit::URI& uri) const {

    /// @todo: revisit the name of this method


    /// @note: code for bucket per DB
    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    const auto pn = parts.size();
    ASSERT(pn == 1 | pn == 2);
    ASSERT(parts[0] == db_bucket_);
    ASSERT(uri.scheme() == type());
    eckit::S3BucketName n{eckit::net::Endpoint{uri.host(), uri.port()}, parts[0]};
    return n.exists();


    /// @note: code for single bucket for all DBs
    // ASSERT(uri.scheme() == type());
    // eckit::S3Name n(uri);
    // ASSERT(n.bucket().name() == bucket_);
    // ASSERT(n.name().rfind(db_prefix_, 0) == 0);
    // return n.exists();

}

std::vector<eckit::URI> S3Store::storeUnitURIs() const {

    std::vector<eckit::URI> store_unit_uris;

    
    eckit::S3BucketName bucket {endpoint_, db_bucket_};
    if (!bucket.exists()) return store_unit_uris;
    
    /// @note if an S3Catalogue is implemented, some filtering will need to
    ///   be done here to discriminate store keys from catalogue keys
    for (const auto& key : bucket.listObjects()) {

        store_unit_uris.push_back(bucket.makeObject(key)->uri());

    }

    return store_unit_uris;


    /// @note: code for single bucket for all DBs
    // std::vector<eckit::URI> store_unit_uris;

    // eckit::S3Bucket bucket{endpoint_, bucket_};
    
    // if (!bucket.exists()) return store_unit_uris;
    
    // /// @note if an S3Catalogue is implemented, more filtering will need to
    // ///   be done here to discriminate store keys from catalogue keys
    // for (const auto& key : bucket.listObjects(filter = "^" + db_prefix_ + "_.*")) {

    //     store_unit_uris.push_back(key.uri());

    // }

    // return store_unit_uris;

}

std::set<eckit::URI> S3Store::asStoreUnitURIs(const std::vector<eckit::URI>& uris) const {

    std::set<eckit::URI> res;

    /// @note: this is only uniquefying the input uris (coming from an index) 
    ///   in case theres any duplicate.
    for (auto& uri : uris) 
        res.insert(uri);

    return res;

}

bool S3Store::exists() const {


    return eckit::S3BucketName(endpoint_, db_bucket_).exists();
}

/// @todo: never used in actual fdb-read?
eckit::DataHandle* S3Store::retrieve(Field& field) const {

    return field.dataHandle();

}

std::unique_ptr<FieldLocation> S3Store::archive(const Key& key, const void * data, eckit::Length length) {

    /// @note: code for S3 object (key) per field:

    /// @note: generate unique key name
    ///  if single bucket, starting by dbkey_indexkey_
    ///  if bucket per db, starting by indexkey_
    eckit::S3ObjectName n = generateDataKey(key);

    /// @todo: ensure bucket if not yet seen by this process
    static std::set<std::string> knownBuckets;
    if (knownBuckets.find(n.bucket()) == knownBuckets.end()) {
        eckit::S3BucketName(n.endpoint(), n.bucket()).ensureCreated();
        knownBuckets.insert(n.bucket());
    }

    std::unique_ptr<eckit::DataHandle> h(n.dataHandle());

    h->openForWrite(length);
    eckit::AutoClose closer(*h);

    h->write(data, length);

    return std::unique_ptr<S3FieldLocation>(new S3FieldLocation(n.uri(), 0, length, fdb5::Key()));


    /// @note: code for S3 object (key) per index store:

    // /// @note: get or generate unique key name
    // ///  if single bucket, starting by dbkey_indexkey_
    // ///  if bucket per db, starting by indexkey_
    // eckit::S3Name n = getDataKey(key);

    // eckit::DataHandle &dh = getDataHandle(key, n);

    // eckit::Offset offset{dh.position()};

    // h.write(data, length);

    // return std::unique_ptr<S3FieldLocation>(new S3FieldLocation(n.URI(), offset, length, fdb5::Key()));

}

void S3Store::flush() {

    /// @note: code for S3 object (key) per index store:
    
    // /// @note: clear cached data handles thus triggering consolidation of 
    // ///   multipart objects, so that step data is made visible to readers. 
    // ///   New S3 handles will be created on the next archive() call after 
    // ///   flush().
    // closeDataHandles();

}

void S3Store::close() {

    /// @note: code for S3 object (key) per index store:

    // closeDataHandles();

}

void S3Store::remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const {

    /// @note: code for bucket per DB

    const auto parts = eckit::Tokenizer("/").tokenize(uri.name());
    const auto n = parts.size();
    ASSERT(n == 1 | n == 2);
    
    ASSERT(parts[0] == db_bucket_);

    if (n == 2) {  // object

        eckit::S3ObjectName key {uri};

        logVerbose << "destroy S3 key: " << key.asString() << std::endl;

        if (doit) { key.remove(); }

    } else {  // pool

        eckit::S3BucketName bucket {uri};

        logVerbose << "destroy S3 bucket: " << bucket.asString() << std::endl;

        if (doit) bucket.ensureDestroyed();
    }


    // /// @note: code for single bucket for all DBs
    // eckit::S3Name n{uri};
    
    // ASSERT(n.bucket().name() == bucket_);
    // /// @note: if uri doesn't have key name, maybe this method should return without destroying anything.
    // ///   this way when TocWipeVisitor has wipeAll == true, the (only) bucket will not be destroyed
    // ASSERT(n.name().rfind(db_prefix_, 0) == 0);

    // logVerbose << "destroy S3 key: ";
    // logAlways << n.asString() << std::endl;
    // if (doit) n.destroy();

}

void S3Store::print(std::ostream& out) const {

    out << "S3Store(" << endpoint_ << "/" << db_bucket_ << ")";

    /// @note: code for single bucket for all DBs
    // out << "S3Store(" << endpoint_ << "/" << bucket_ << ")";

}

/// @note: unique name generation copied from LocalPathName::unique.
static eckit::StaticMutex local_mutex;


eckit::S3ObjectName S3Store::generateDataKey(const Key& key) const {
    eckit::AutoLock<eckit::StaticMutex> lock(local_mutex);

    std::string hostname = eckit::Main::hostname();

    static unsigned long long n = (((unsigned long long)::getpid()) << 32);

    static std::string format = "%Y%m%d.%H%M%S";
    std::ostringstream os;
    os << eckit::TimeStamp(format) << '.' << hostname << '.' << n++;

    std::string name = os.str();

    while (::access(name.c_str(), F_OK) == 0) {
        std::ostringstream os;
        os << eckit::TimeStamp(format) << '.' << hostname << '.' << n++;
        name = os.str();
    }

    eckit::MD5 md5(name);

    std::string keyStr = key.valuesToString();
    std::replace(keyStr.begin(), keyStr.end(), ':', '-');

    return eckit::S3ObjectName {endpoint_, db_bucket_, keyStr + "." + md5.digest() + ".data"};

    /// @note: code for single bucket for all DBs
    // return eckit::S3Name{endpoint_, bucket_, db_prefix_ + "_" + key.valuesToString() + "_" + md5.digest() + ".data"};

}

/// @note: code for S3 object (key) per index store:
// eckit::S3Name S3Store::getDataKey(const Key& key) const {

//     KeyStore::const_iterator j = dataKeys_.find(key);

//     if ( j != dataKeys_.end() )
//         return j->second;

//     eckit::S3Name dataKey = generateDataKey(key);

//     dataKeys_[ key ] = dataKey;

//     return dataKey;

// }

/// @note: code for S3 object (key) per index store:
// eckit::DataHandle& S3Store::getDataHandle(const Key& key, const eckit::S3Name& name) {

//     HandleStore::const_iterator j = handles_.find(key);
//     if ( j != handles_.end() )
//         return j->second;

//     eckit::DataHandle *dh = name.dataHandle(multipart = true);
    
//     ASSERT(dh);

//     handles_[ key ] = dh;

//     dh->openForAppend(0);
    
//     return *dh;

// }

/// @note: code for S3 object (key) per index store:
// void S3Store::closeDataHandles() {

//     for ( HandleStore::iterator j = handles_.begin(); j != handles_.end(); ++j ) {
//         eckit::DataHandle *dh = j->second;
//         dh->close();
//         delete dh;
//     }

//     handles_.clear();

// }

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
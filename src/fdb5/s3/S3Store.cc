/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/s3/S3Store.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/Length.h"
#include "eckit/io/s3/S3BucketName.h"
#include "eckit/io/s3/S3Name.h"
#include "eckit/log/TimeStamp.h"
#include "eckit/runtime/Main.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/StaticMutex.h"
#include "eckit/utils/MD5.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Store.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/s3/S3Common.h"
#include "fdb5/s3/S3FieldLocation.h"

#include <unistd.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static StoreBuilder<S3Store> builder("s3");

S3Store::S3Store(const Schema& schema, const Key& key, const Config& config)
    : Store(schema), S3Common(config, "store", key), config_(config) { }

S3Store::S3Store(const Schema& schema, const eckit::URI& uri, const Config& config)
    : Store(schema), S3Common(config, "store", uri), config_(config) { }

eckit::URI S3Store::uri() const {
    return eckit::S3BucketName(endpoint_, db_bucket_).uri();
}

bool S3Store::uriBelongs(const eckit::URI& uri) const {
    const auto uriBucket = eckit::S3Name::parse(uri.name())[0];
    return uri.scheme() == type() && uriBucket == db_bucket_;
}

bool S3Store::uriExists(const eckit::URI& uri) const {
    return eckit::S3Name::make(endpoint_, uri.name())->exists();
}

bool S3Store::auxiliaryURIExists(const eckit::URI& uri) const {
    return uriExists(uri);
}

std::vector<eckit::URI> S3Store::collocatedDataURIs() const {

    std::vector<eckit::URI> store_unit_uris;

    eckit::S3BucketName bucket {endpoint_, db_bucket_};
    if (!bucket.exists()) { return store_unit_uris; }

    /// @note if an S3Catalogue is implemented, some filtering will need to
    ///   be done here to discriminate store keys from catalogue keys
    for (const auto& key : bucket.listObjects()) { store_unit_uris.push_back(bucket.makeObject(key)->uri()); }

    return store_unit_uris;
}

std::set<eckit::URI> S3Store::asCollocatedDataURIs(const std::vector<eckit::URI>& uris) const {
    return {uris.begin(), uris.end()};
}

bool S3Store::exists() const {
    return eckit::S3BucketName(endpoint_, db_bucket_).exists();
}

eckit::URI S3Store::getAuxiliaryURI(const eckit::URI& uri, const std::string& ext) const {
    ASSERT(uri.scheme() == type());
    return {type(), uri.name() + '.' + ext};
}

std::vector<eckit::URI> S3Store::getAuxiliaryURIs(const eckit::URI& uri) const {
    ASSERT(uri.scheme() == type());
    std::vector<eckit::URI> uris;
    for (const auto& ext : LibFdb5::instance().auxiliaryRegistry()) { uris.push_back(getAuxiliaryURI(uri, ext)); }
    return uris;
}

//----------------------------------------------------------------------------------------------------------------------

/// @todo: never used in actual fdb-read?
eckit::DataHandle* S3Store::retrieve(Field& field) const {
    return field.dataHandle();
}

std::unique_ptr<const FieldLocation> S3Store::archive(const Key& key, const void* data, eckit::Length length) {

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

    return std::unique_ptr<const S3FieldLocation>(new S3FieldLocation(n.uri(), 0, length, fdb5::Key()));
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

    auto item = eckit::S3Name::make(endpoint_, uri.name());

    if (auto* object = dynamic_cast<eckit::S3ObjectName*>(item.get())) {
        logVerbose << "Removing S3 object: " << object->asString() << '\n';
        if (doit) { object->remove(); }
    } else if (auto* bucket = dynamic_cast<eckit::S3BucketName*>(item.get())) {
        logVerbose << "Removing S3 bucket: " << bucket->asString() << '\n';
        if (doit) { bucket->ensureDestroyed(); }
    } else {
        throw eckit::SeriousBug("S3Store::remove: unknown URI type: " + uri.asString(), Here());
    }
}

void S3Store::print(std::ostream& out) const {
    out << "S3Store(" << endpoint_ << "/" << db_bucket_ << ")";
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

    return eckit::S3ObjectName {endpoint_, {db_bucket_, keyStr + "." + md5.digest() + ".data"}};
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Bytes.h"
#include "eckit/log/Timer.h"

#include "eckit/config/Resource.h"
#include "eckit/io/EmptyHandle.h"
#include "eckit/io/rados/RadosWriteHandle.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/io/FDBFileHandle.h"
#include "fdb5/rados/RadosFieldLocation.h"
#include "fdb5/rados/RadosStore.h"
#include "fdb5/rules/Rule.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RadosStore::RadosStore(const Key& key, const Config& config) :
    Store(), directory_("mars:" + key.valuesToString()), archivedFields_(0) {}

RadosStore(const Key& key, const Config& config, const eckit::net::Endpoint& controlEndpoint) :
    Store(), directory_("mars:" + key.valuesToString()), archivedFields_(0) {
    NOTIMP;
}

RadosStore::RadosStore(const eckit::URI& uri) :
    Store(), directory_("mars:" + uri.path().dirName()), archivedFields_(0) {}

eckit::URI RadosStore::uri() const {
    return URI("rados", directory_);
}

bool RadosStore::exists() const {
    return true;
}

eckit::DataHandle* RadosStore::retrieve(Field& field, Key& remapKey) const {
    return remapKey.empty() ? field.dataHandle() : field.dataHandle(remapKey);
}

std::unique_ptr<const FieldLocation> RadosStore::archive(const uint32_t, const Key& key, const void* data,
                                                         eckit::Length length) {
    archivedFields_++;

    eckit::PathName dataPath = getDataPath(key);
    eckit::URI dataUri("rados", dataPath);

    eckit::DataHandle& dh = getDataHandle(dataPath);

    eckit::Offset position = dh.position();

    long len = dh.write(data, length);

    ASSERT(len == length);

    return std::make_unique<const RadosFieldLocation>(dataUri, position, length);
}

size_t RadosStore::flush() {
    if (archivedFields_ == 0) {
        return 0;
    }

    // ensure consistent state before writing Toc entry

    flushDataHandles();

    size_t out      = archivedFields_;
    archivedFields_ = 0;
    return out;
}

void RadosStore::close() {
    closeDataHandles();
}

void RadosStore::remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const {
    ASSERT(uri.scheme() == type());

    eckit::PathName path = uri.path();
    if (path.isDir()) {
        logVerbose << "rmdir: ";
        logAlways << path << std::endl;
        if (doit)
            path.rmdir(false);
    }
    else {
        logVerbose << "Unlinking: ";
        logAlways << path << std::endl;
        if (doit)
            path.unlink(false);
    }
}

eckit::DataHandle* RadosStore::getCachedHandle(const eckit::PathName& path) const {
    HandleStore::const_iterator j = handles_.find(path);
    if (j != handles_.end())
        return j->second;
    else
        return nullptr;
}

void RadosStore::closeDataHandles() {
    for (HandleStore::iterator j = handles_.begin(); j != handles_.end(); ++j) {
        eckit::DataHandle* dh = j->second;
        dh->close();
        delete dh;
    }
    handles_.clear();
}

eckit::DataHandle* RadosStore::createFileHandle(const eckit::PathName& path) {

    //    static size_t sizeBuffer = eckit::Resource<unsigned long>("fdbBufferSize", 64 * 1024 * 1024);

    LOG_DEBUG_LIB(LibFdb5) << "Creating RadosWriteHandle to "
                           << path
                           //                                 << " with buffer of " << eckit::Bytes(sizeBuffer)
                           << std::endl;

    return new RadosWriteHandle(path, 0);
}

eckit::DataHandle* RadosStore::createAsyncHandle(const eckit::PathName& path) {
    NOTIMP;

    /*    static size_t nbBuffers  = eckit::Resource<unsigned long>("fdbNbAsyncBuffers", 4);
        static size_t sizeBuffer = eckit::Resource<unsigned long>("fdbSizeAsyncBuffer", 64 * 1024 * 1024);

        return new eckit::AIOHandle(path, nbBuffers, sizeBuffer);*/
}

eckit::DataHandle* RadosStore::createDataHandle(const eckit::PathName& path) {

    static bool fdbWriteToNull = eckit::Resource<bool>("fdbWriteToNull;$FDB_WRITE_TO_NULL", false);
    if (fdbWriteToNull)
        return new eckit::EmptyHandle();

    static bool fdbAsyncWrite = eckit::Resource<bool>("fdbAsyncWrite;$FDB_ASYNC_WRITE", false);
    if (fdbAsyncWrite)
        return createAsyncHandle(path);

    return createFileHandle(path);
}

eckit::DataHandle& RadosStore::getDataHandle(const eckit::PathName& path) {
    eckit::DataHandle* dh = getCachedHandle(path);
    if (!dh) {
        dh = createDataHandle(path);
        ASSERT(dh);
        handles_[path] = dh;
        dh->openForWrite(0);
    }
    return *dh;
}

eckit::PathName RadosStore::generateDataPath(const Key& key) const {

    eckit::PathName dpath(directory_);
    dpath /= key.valuesToString();
    dpath = eckit::PathName::unique(dpath) + ".data";
    return dpath;
}

eckit::PathName RadosStore::getDataPath(const Key& key) {
    PathStore::const_iterator j = dataPaths_.find(key);
    if (j != dataPaths_.end())
        return j->second;

    eckit::PathName dataPath = generateDataPath(key);

    dataPaths_[key] = dataPath;

    return dataPath;
}

void RadosStore::flushDataHandles() {

    for (HandleStore::iterator j = handles_.begin(); j != handles_.end(); ++j) {
        eckit::DataHandle* dh = j->second;
        dh->flush();
    }
}

void RadosStore::print(std::ostream& out) const {
    out << "RadosStore(" << directory_ << ")";
}

static StoreBuilder<RadosStore> builder("rados");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

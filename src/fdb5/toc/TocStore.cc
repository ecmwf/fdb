/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Timer.h"

#include "eckit/config/Resource.h"
#include "eckit/io/AIOHandle.h"
#include "eckit/io/EmptyHandle.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/toc/TocFieldLocation.h"
#include "fdb5/toc/RootManager.h"
#include "fdb5/toc/TocPurgeVisitor.h"
#include "fdb5/toc/TocStats.h"
#include "fdb5/toc/TocStore.h"
#include "fdb5/io/FDBFileHandle.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocStore::TocStore(const Schema& schema, const Key& key, const Config& config) :
    Store(schema), TocCommon(TocCommon::getDirectory(key, config)) {}

TocStore::TocStore(const Schema& schema, const eckit::URI& uri, const Config& config) :
    Store(schema), TocCommon(uri.path().dirName()) {}

eckit::URI TocStore::uri() const {
    return URI("file", directory_);
}

bool TocStore::exists() const {
    return directory_.exists();
}

eckit::DataHandle* TocStore::retrieve(Field& field, Key& remapKey) const {
    return remapKey.empty() ?
        field.dataHandle() :
        field.dataHandle(remapKey);
}

FieldLocation* TocStore::archive(const Key &key, const void *data, eckit::Length length) {
    dirty_ = true;

    eckit::PathName dataPath = getDataPath(key);
    eckit::URI dataUri("file", dataPath);

    eckit::DataHandle &dh = getDataHandle(dataPath);

    eckit::Offset position = dh.position();

    long len = dh.write( data, length );

    ASSERT(len == length);

    return new TocFieldLocation(dataUri, position, length);
}

void TocStore::flush() {
    if (!dirty_) {
        return;
    }

    // ensure consistent state before writing Toc entry

    flushDataHandles();

    dirty_ = false;
}

void TocStore::close() {
    closeDataHandles();
}

void TocStore::remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const {
    ASSERT(uri.scheme() == type());

    eckit::PathName path = uri.path();
    if (path.isDir()) {
        logVerbose << "rmdir: ";
        logAlways << path << std::endl;
        if (doit) path.rmdir(false);
    } else {
        logVerbose << "Unlinking: ";
        logAlways << path << std::endl;
        if (doit) path.unlink(false);
    }
}

eckit::DataHandle *TocStore::getCachedHandle( const eckit::PathName &path ) const {
    HandleStore::const_iterator j = handles_.find( path );
    if ( j != handles_.end() )
        return j->second;
    else
        return nullptr;
}

void TocStore::closeDataHandles() {
    for ( HandleStore::iterator j = handles_.begin(); j != handles_.end(); ++j ) {
        eckit::DataHandle *dh = j->second;
        dh->close();
        delete dh;
    }
    handles_.clear();
}


LustreStripe TocStore::stripeDataLustreSettings() {

    static unsigned int fdbDataLustreStripeCount = eckit::Resource<unsigned int>("fdbDataLustreStripeCount;$FDB_DATA_LUSTRE_STRIPE_COUNT", 8);
    static size_t fdbDataLustreStripeSize = eckit::Resource<size_t>("fdbDataLustreStripeSize;$FDB_DATA_LUSTRE_STRIPE_SIZE", 8*1024*1024);

    return LustreStripe(fdbDataLustreStripeCount, fdbDataLustreStripeSize);
}

eckit::DataHandle *TocStore::createFileHandle(const eckit::PathName &path) {

    static size_t sizeBuffer = eckit::Resource<unsigned long>("fdbBufferSize", 64 * 1024 * 1024);

    if(stripeLustre()) {

        eckit::Log::debug<LibFdb5>() << "Creating LustreFileHandle<FDBFileHandle> to " << path
                                     << " buffer size " << sizeBuffer
                                     << std::endl;

        return new LustreFileHandle<FDBFileHandle>(path, sizeBuffer, stripeDataLustreSettings());
    }

    eckit::Log::debug<LibFdb5>() << "Creating FDBFileHandle to " << path
                                 << " with buffer of " << eckit::Bytes(sizeBuffer)
                                 << std::endl;

    return new FDBFileHandle(path, sizeBuffer);
}

eckit::DataHandle *TocStore::createAsyncHandle(const eckit::PathName &path) {

    static size_t nbBuffers  = eckit::Resource<unsigned long>("fdbNbAsyncBuffers", 4);
    static size_t sizeBuffer = eckit::Resource<unsigned long>("fdbSizeAsyncBuffer", 64 * 1024 * 1024);

    if(stripeLustre()) {

        eckit::Log::debug<LibFdb5>() << "Creating LustreFileHandle<AIOHandle> to " << path
                                     << " with " << nbBuffers
                                     << " buffer each with " << eckit::Bytes(sizeBuffer)
                                     << std::endl;

        return new LustreFileHandle<eckit::AIOHandle>(path, nbBuffers, sizeBuffer, stripeDataLustreSettings());
    }

    return new eckit::AIOHandle(path, nbBuffers, sizeBuffer);
}

eckit::DataHandle *TocStore::createDataHandle(const eckit::PathName &path) {

    static bool fdbWriteToNull = eckit::Resource<bool>("fdbWriteToNull;$FDB_WRITE_TO_NULL", false);
    if(fdbWriteToNull)
        return new eckit::EmptyHandle();

    static bool fdbAsyncWrite = eckit::Resource<bool>("fdbAsyncWrite;$FDB_ASYNC_WRITE", false);
    if(fdbAsyncWrite)
        return createAsyncHandle(path);

    return createFileHandle(path);
}

eckit::DataHandle& TocStore::getDataHandle( const eckit::PathName &path ) {
    eckit::DataHandle *dh = getCachedHandle(path);
    if ( !dh ) {
        dh = createDataHandle(path);
        ASSERT(dh);
        handles_[path] = dh;
        dh->openForAppend(0);
    }
    return *dh;
}

eckit::PathName TocStore::generateDataPath(const Key &key) const {

    eckit::PathName dpath ( directory_ );
    dpath /=  key.valuesToString();
    dpath = eckit::PathName::unique(dpath) + ".data";
    return dpath;
}

eckit::PathName TocStore::getDataPath(const Key &key) {
    PathStore::const_iterator j = dataPaths_.find(key);
    if ( j != dataPaths_.end() )
        return j->second;

    eckit::PathName dataPath = generateDataPath(key);

    dataPaths_[ key ] = dataPath;

    return dataPath;
}

void TocStore::flushDataHandles() {

    for (HandleStore::iterator j = handles_.begin(); j != handles_.end(); ++j) {
        eckit::DataHandle *dh = j->second;
        dh->flush();
    }
}

void TocStore::print(std::ostream &out) const {
    out << "TocStore(" << directory_ << ")";
}

static StoreBuilder<TocStore> builder("file");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

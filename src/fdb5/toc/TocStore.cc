/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <dirent.h>
#include <fcntl.h>

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
#include "fdb5/io/LustreFileHandle.h"

#include "fdb5/toc/TocSession.h"
#include "fdb5/toc/TocIOStats.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocStore::TocStore(const Schema& schema, const Key& key, const Config& config) :
    Store(schema), TocCommon(StoreRootManager(config).directory(key).directory_) {}

eckit::URI TocStore::uri() const {

    return URI("file", directory_);

}

bool TocStore::uriBelongs(const eckit::URI& uri) const {

    // TODO: assert uri represents a (not necessarily existing) data file
    return ((uri.scheme() == type()) && (uri.path().dirName().sameAs(directory_)));

}

bool TocStore::uriExists(const eckit::URI& uri) const {

    ASSERT(uri.scheme() == type());
    eckit::PathName p(uri.path());
    // ensure provided URI is either DB URI or Store file URI
    if (!p.sameAs(directory_)) {
        ASSERT(p.dirName().sameAs(directory_));
        ASSERT(p.extension() == ".data");
    }

    return p.exists();

}

std::vector<eckit::URI> TocStore::collocatedDataURIs() const {

    std::vector<eckit::PathName> files;
    std::vector<eckit::PathName> dirs;
    (directory_).children(files, dirs);

    std::vector<eckit::URI> res;
    for (const auto& f : files) {
        if (f.extension() == ".data") {
            res.push_back(eckit::URI{type(), f});
        }
    }

    return res;

}

std::set<eckit::URI> TocStore::asCollocatedDataURIs(const std::vector<eckit::URI>& uris) const {

    std::set<eckit::URI> res;

    for (auto& uri : uris) {

        ASSERT(uri.path().extension() == ".data");
        res.insert(uri);

    }

    return res;

}

bool TocStore::exists() const {

    return directory_.exists();

}

eckit::DataHandle* TocStore::retrieve(Field& field) const {
    return field.dataHandle();
}

std::unique_ptr<FieldLocation> TocStore::archive(const Key &key, const void *data, eckit::Length length) {

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::TocManager::instance().timer();
    fdb5::TocIOStats& stats = fdb5::TocManager::instance().stats();

    fdb5::TocStatsTimer st{"archive 005 TocStore::archive", timer, std::bind(&fdb5::TocIOStats::logMdOperation, &stats, _1, _2)};

    dirty_ = true;

    eckit::PathName dataPath = getDataPath(key);

    eckit::DataHandle &dh = getDataHandle(dataPath);

    eckit::Offset position = dh.position();

    long len = dh.write( data, length );

    ASSERT(len == length);

    return std::unique_ptr<TocFieldLocation>(new TocFieldLocation(dataPath, position, length, Key(nullptr, true)));
}

void TocStore::flush() {
    if (!dirty_) {
        return;
    }

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::TocManager::instance().timer();
    fdb5::TocIOStats& stats = fdb5::TocManager::instance().stats();

    fdb5::TocStatsTimer st{"archive 007 TocStore::flush", timer, std::bind(&fdb5::TocIOStats::logMdOperation, &stats, _1, _2)};

    // ensure consistent state before writing Toc entry

    flushDataHandles();

    dirty_ = false;
}

void TocStore::close() {

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::TocManager::instance().timer();
    fdb5::TocIOStats& stats = fdb5::TocManager::instance().stats();

    fdb5::TocStatsTimer st{"archive 010 TocStore::close", timer, std::bind(&fdb5::TocIOStats::logMdOperation, &stats, _1, _2)};

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

    using namespace std::placeholders;
    eckit::Timer& timer = fdb5::TocManager::instance().timer();
    fdb5::TocIOStats& stats = fdb5::TocManager::instance().stats();

    fdb5::TocStatsTimer st{"retrieve 007 TocStore::closeDataHandles", timer, std::bind(&fdb5::TocIOStats::logMdOperation, &stats, _1, _2)};
    
    for ( HandleStore::iterator j = handles_.begin(); j != handles_.end(); ++j ) {
        eckit::DataHandle *dh = j->second;
        dh->close();
        delete dh;
    }
    handles_.clear();
}

eckit::DataHandle *TocStore::createFileHandle(const eckit::PathName &path) {

    static size_t sizeBuffer = eckit::Resource<unsigned long>("fdbBufferSize", 64 * 1024 * 1024);

    if(stripeLustre()) {

        LOG_DEBUG_LIB(LibFdb5) << "Creating LustreFileHandle<FDBFileHandle> to " << path
                                     << " buffer size " << sizeBuffer
                                     << std::endl;

        return new LustreFileHandle<FDBFileHandle>(path, sizeBuffer, stripeDataLustreSettings());
    }

    LOG_DEBUG_LIB(LibFdb5) << "Creating FDBFileHandle to " << path
                                 << " with buffer of " << eckit::Bytes(sizeBuffer)
                                 << std::endl;

    return new FDBFileHandle(path, sizeBuffer);
}

eckit::DataHandle *TocStore::createAsyncHandle(const eckit::PathName &path) {

    static size_t nbBuffers  = eckit::Resource<unsigned long>("fdbNbAsyncBuffers", 4);
    static size_t sizeBuffer = eckit::Resource<unsigned long>("fdbSizeAsyncBuffer", 64 * 1024 * 1024);

    if(stripeLustre()) {

        LOG_DEBUG_LIB(LibFdb5) << "Creating LustreFileHandle<AIOHandle> to " << path
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
    /// @todo: in cases where a catalogue other than POSIX is used and a
    ///   POSIX store is used, the DB directory for the store is first 
    ///   created within PathName::unique().
    ///   DB directory creation should maybe be removed from there and 
    ///   performed here, or as part of FDB/LustreFileHandle::openForAppend
    ///   if not exists.
    ///   If doing it in openForAppend, it should be ensured that the
    ///   existence of the database directory is not checked an excessive
    ///   amount of times.
    dpath = eckit::PathName::unique(dpath) + ".data";
    return dpath;
}

eckit::PathName TocStore::getDataPath(const Key &key) const {
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

bool TocStore::canMoveTo(const Key& key, const Config& config, const eckit::URI& dest) const {
    if (dest.scheme().empty() || dest.scheme() == "toc" || dest.scheme() == "file" || dest.scheme() == "unix") {
        eckit::PathName destPath = dest.path();
        for (const eckit::PathName& root: StoreRootManager(config).canMoveToRoots(key)) {
            if (root.sameAs(destPath)) {
                return true;
            }
        }
    }
    std::stringstream ss;
    ss << "Destination " << dest << " cannot be used to archive a DB with key: " << key << std::endl;
    throw eckit::UserError(ss.str(), Here());
}

// void mpiCopyTask(const eckit::PathName& srcPath, const eckit::PathName& destPath, const std::string& fileName) {

//     eckit::PathName src_ = srcPath / fileName;
//     eckit::PathName dest_ = destPath / fileName;

//     eckit::FileHandle src(src_);
//     eckit::FileHandle dest(dest_);
//     src.copyTo(dest);
// }

void TocStore::moveTo(const Key& key, const Config& config, const eckit::URI& dest, eckit::Queue<MoveElement>& queue) const {
    eckit::PathName destPath = dest.path();
    for (const eckit::PathName& root: StoreRootManager(config).canMoveToRoots(key)) {
        if (root.sameAs(destPath)) {      
            eckit::PathName src_db = directory_;
            eckit::PathName dest_db = destPath / key.valuesToString();

            dest_db.mkdir();
            DIR* dirp = ::opendir(src_db.asString().c_str());
            struct dirent* dp;
            std::multimap<long, FileCopy*, std::greater<long>> files;
            while ((dp = ::readdir(dirp)) != NULL) {
                if (strstr( dp->d_name, ".data")) {
                    eckit::PathName file(src_db / dp->d_name);
                    struct stat fileStat;
                    ::stat(file.asString().c_str(), &fileStat);
                    files.emplace(fileStat.st_size, new FileCopy(src_db.path(), dest_db, dp->d_name));
                }
            }
            closedir(dirp);

            for (auto it = files.begin(); it != files.end(); it++) {
                queue.emplace(*(it->second));
            }
        }
    }
}

void TocStore::remove(const Key& key) const {

    eckit::PathName src_db = directory_;
        
    DIR* dirp = ::opendir(src_db.asString().c_str());
    struct dirent* dp;
    while ((dp = ::readdir(dirp)) != NULL) {
        if (strstr( dp->d_name, ".data")) {
            eckit::PathName dataFile = src_db / dp->d_name;
            LOG_DEBUG_LIB(LibFdb5) << "Removing " << dataFile << std::endl;
            dataFile.unlink(false);
        }
    }
    closedir(dirp);
}

void TocStore::print(std::ostream &out) const {
    out << "TocStore(" << directory_ << ")";
}

static StoreBuilder<TocStore> builder("file");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

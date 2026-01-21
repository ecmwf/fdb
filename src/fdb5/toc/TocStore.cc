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
#include <memory>

#include "eckit/log/Timer.h"

#include "eckit/config/Resource.h"
#include "eckit/io/AIOHandle.h"
#include "eckit/io/EmptyHandle.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/WipeIterator.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/WipeState.h"
#include "fdb5/io/FDBFileHandle.h"
#include "fdb5/io/LustreFileHandle.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/toc/RootManager.h"
#include "fdb5/toc/TocFieldLocation.h"
#include "fdb5/toc/TocPurgeVisitor.h"
#include "fdb5/toc/TocStats.h"
#include "fdb5/toc/TocStore.h"

using namespace eckit;

namespace {

eckit::PathName directory(const eckit::PathName& dir) {
    if (dir.isDir()) {
        return dir;
    }
    return dir.dirName();
}

}  // namespace

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocStore::TocStore(const Key& key, const Config& config) :
    Store(),
    TocCommon(StoreRootManager(config).directory(key).directory_),
    archivedFields_(0),
    auxFileExtensions_{auxFileExtensions()} {}

TocStore::TocStore(const eckit::URI& uri, const Config& config) :
    Store(), TocCommon(directory(uri.path())), archivedFields_(0), auxFileExtensions_{auxFileExtensions()} {}

eckit::URI TocStore::uri() const {
    return URI(type(), directory_);
}

eckit::URI TocStore::uri(const eckit::URI& dataURI) {
    ASSERT(dataURI.scheme() == "file");
    return URI("file", dataURI.path().dirName());
}

bool TocStore::uriBelongs(const eckit::URI& uri) const {
    if (uri.scheme() != type() || !uri.path().dirName().sameAs(directory_)) {
        return false;
    }

    std::string ext = uri.path().extension();
    if (ext == ".data") {
        return true;
    }

    for (const auto& auxExt : auxFileExtensions_) {
        if (ext == auxExt) {
            return true;
        }
    }

    return false;
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

bool TocStore::auxiliaryURIExists(const eckit::URI& uri) const {
    ASSERT(uri.scheme() == type());
    eckit::PathName p(uri.path());
    ASSERT(p.dirName().sameAs(directory_));
    return p.exists();
}

std::set<eckit::URI> TocStore::collocatedDataURIs() const {

    std::vector<eckit::PathName> files;
    std::vector<eckit::PathName> dirs;
    PathName(directory_).children(files, dirs);

    std::set<eckit::URI> res;
    for (const auto& f : files) {
        if (f.extension() == ".data") {
            res.emplace(type(), f);
        }
    }

    return res;
}

std::set<eckit::URI> TocStore::asCollocatedDataURIs(const std::set<eckit::URI>& uris) const {

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

std::unique_ptr<const FieldLocation> TocStore::archive(const Key& idxKey, const void* data, eckit::Length length) {
    archivedFields_++;

    eckit::PathName dataPath = getDataPath(idxKey);

    eckit::DataHandle& dh = getDataHandle(dataPath);

    eckit::Offset position = dh.position();

    long len = dh.write(data, length);

    ASSERT(len == static_cast<long>(length));

    return std::make_unique<TocFieldLocation>(dataPath, position, length, Key());
}

size_t TocStore::flush() {
    if (archivedFields_ == 0) {
        return 0;
    }

    // ensure consistent state before writing Toc entry
    flushDataHandles();

    size_t out      = archivedFields_;
    archivedFields_ = 0;

    return out;
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

eckit::DataHandle* TocStore::getCachedHandle(const eckit::PathName& path) const {
    std::lock_guard lock(handlesMutex_);
    if (auto j = handles_.find(path); j != handles_.end()) {
        return j->second.get();
    }
    return nullptr;
}

void TocStore::closeDataHandles() {
    std::lock_guard lock(handlesMutex_);
    for (const auto& [p, dh] : handles_) {
        dh->close();
    }
    handles_.clear();
}

std::unique_ptr<eckit::DataHandle> TocStore::createFileHandle(const eckit::PathName& path) {

    static size_t sizeBuffer = eckit::Resource<unsigned long>("fdbBufferSize", 64_MiB);

    if (stripeLustre()) {

        LOG_DEBUG_LIB(LibFdb5) << "Creating LustreFileHandle<FDBFileHandle> to " << path << " buffer size "
                               << sizeBuffer << std::endl;

        return std::make_unique<LustreFileHandle<FDBFileHandle>>(path, sizeBuffer, stripeDataLustreSettings());
    }

    LOG_DEBUG_LIB(LibFdb5) << "Creating FDBFileHandle to " << path << " with buffer of " << eckit::Bytes(sizeBuffer)
                           << std::endl;

    return std::make_unique<FDBFileHandle>(path, sizeBuffer);
}

std::unique_ptr<eckit::DataHandle> TocStore::createAsyncHandle(const eckit::PathName& path) {

    static size_t nbBuffers  = eckit::Resource<unsigned long>("fdbNbAsyncBuffers", 4);
    static size_t sizeBuffer = eckit::Resource<unsigned long>("fdbSizeAsyncBuffer", 64_MiB);

    if (stripeLustre()) {

        LOG_DEBUG_LIB(LibFdb5) << "Creating LustreFileHandle<AIOHandle> to " << path << " with " << nbBuffers
                               << " buffer each with " << eckit::Bytes(sizeBuffer) << std::endl;

        return std::make_unique<LustreFileHandle<eckit::AIOHandle>>(path, nbBuffers, sizeBuffer,
                                                                    stripeDataLustreSettings());
    }

    return std::make_unique<eckit::AIOHandle>(path, nbBuffers, sizeBuffer);
}

std::unique_ptr<eckit::DataHandle> TocStore::createDataHandle(const eckit::PathName& path) {

    static bool fdbWriteToNull = eckit::Resource<bool>("fdbWriteToNull;$FDB_WRITE_TO_NULL", false);
    if (fdbWriteToNull) {
        return std::make_unique<eckit::EmptyHandle>();
    }

    static bool fdbAsyncWrite = eckit::Resource<bool>("fdbAsyncWrite;$FDB_ASYNC_WRITE", false);
    if (fdbAsyncWrite)
        return createAsyncHandle(path);

    return createFileHandle(path);
}

eckit::DataHandle& TocStore::getDataHandle(const eckit::PathName& path) {
    std::lock_guard lock(handlesMutex_);
    eckit::DataHandle* dh = getCachedHandle(path);
    if (dh) {
        return *dh;
    }
    auto dataHandle = createDataHandle(path);
    ASSERT(dataHandle);
    dataHandle->openForAppend(0);
    return *(handles_[path] = std::move(dataHandle));
}

eckit::PathName TocStore::generateDataPath(const Key& key) const {

    eckit::PathName dpath(directory_);
    dpath /= key.valuesToString();
    dpath = eckit::PathName::unique(dpath) + ".data";
    return dpath;
}

eckit::PathName TocStore::getDataPath(const Key& key) const {
    PathStore::const_iterator j = dataPaths_.find(key);
    if (j != dataPaths_.end())
        return j->second;

    eckit::PathName dataPath = generateDataPath(key);

    dataPaths_[key] = dataPath;

    return dataPath;
}

void TocStore::flushDataHandles() {
    std::lock_guard lock(handlesMutex_);
    for (const auto& [p, dh] : handles_) {
        dh->flush();
    }
}

bool TocStore::canMoveTo(const Key& key, const Config& config, const eckit::URI& dest) const {
    if (dest.scheme().empty() || dest.scheme() == "toc" || dest.scheme() == "file" || dest.scheme() == "unix") {
        eckit::PathName destPath = dest.path();
        for (const eckit::PathName& root : StoreRootManager(config).canMoveToRoots(key)) {
            if (root.sameAs(destPath)) {
                return true;
            }
        }
    }
    std::ostringstream ss;
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

eckit::URI TocStore::getAuxiliaryURI(const eckit::URI& uri, const std::string& ext) const {
    // Filebackend: ext is a suffix to append to the file name
    ASSERT(uri.scheme() == type());
    eckit::PathName path = uri.path() + "." + ext;
    return eckit::URI(type(), path);
}

std::vector<eckit::URI> TocStore::getAuxiliaryURIs(const eckit::URI& uri, bool onlyExisting) const {
    ASSERT(uri.scheme() == type());
    std::vector<eckit::URI> uris;
    for (const auto& ext : LibFdb5::instance().auxiliaryRegistry()) {
        auto auxURI = getAuxiliaryURI(uri, ext);
        if (!onlyExisting || auxiliaryURIExists(auxURI)) {
            uris.push_back(auxURI);
        }
    }
    return uris;
}

std::set<std::string> TocStore::auxFileExtensions() const {
    std::set<std::string> extensions;
    for (const auto& e : LibFdb5::instance().auxiliaryRegistry()) {
        extensions.insert("." + e);
    }
    return extensions;
}

void TocStore::moveTo(const Key& key, const Config& config, const eckit::URI& dest,
                      eckit::Queue<MoveElement>& queue) const {
    eckit::PathName destPath = dest.path();

    for (const eckit::PathName& root : StoreRootManager(config).canMoveToRoots(key)) {
        if (root.sameAs(destPath)) {
            eckit::PathName src_db  = directory_;
            eckit::PathName dest_db = destPath / key.valuesToString();

            dest_db.mkdir();
            DIR* dirp = ::opendir(src_db.asString().c_str());
            struct dirent* dp;
            std::multimap<long, std::unique_ptr<FileCopy>, std::greater<long>> files;
            while ((dp = ::readdir(dirp)) != NULL) {
                if (strstr(dp->d_name, ".data")) {
                    eckit::PathName file(src_db / dp->d_name);
                    if ((file.extension() == ".data") ||
                        auxFileExtensions_.find(file.extension()) != auxFileExtensions_.end()) {
                        struct stat fileStat;
                        SYSCALL(::stat(file.asString().c_str(), &fileStat));
                        files.emplace(fileStat.st_size, std::make_unique<FileCopy>(src_db.path(), dest_db, dp->d_name));
                    }
                }
            }
            closedir(dirp);

            for (auto it = files.begin(); it != files.end(); it++) {
                queue.emplace(*(it->second));
            }
        }
    }
}

void TocStore::prepareWipe(StoreWipeState& storeState, bool doit, bool unsafeWipeAll) {

    // Note: doit and unsafeWipeAll do not affect the preparation of a local toc store wipe.

    const std::set<eckit::URI>& dataURIs = storeState.includedDataURIs();  // included according to cat
    const std::set<eckit::URI>& safeURIs = storeState.safeURIs();          // excluded according to cat

    std::set<eckit::URI> nonExistingURIs;
    for (auto& uri : dataURIs) {

        // XXX - this prints an error but never raises? Is this intentional?
        if (!uriBelongs(uri)) {
            Log::error() << "Index to be deleted has pointers to fields that don't belong to the configured store."
                         << std::endl;
            Log::error() << "Configured Store URI: " << this->uri().asString() << std::endl;
            Log::error() << "Pointed Store unit URI: " << uri.asString() << std::endl;
            Log::error() << "Impossible to delete such fields. Index deletion aborted to avoid leaking fields."
                         << std::endl;
            return;
        }

        for (const auto& aux : getAuxiliaryURIs(uri, true)) {
            storeState.insertAuxiliaryURI(aux);
        }

        // URI may not exist (e.g. due to a prior incomplete wipe.)
        if (!uri.path().exists()) {
            nonExistingURIs.insert(uri);
        }
    }

    for (const auto& uri : nonExistingURIs) {
        storeState.unincludeURI(uri);
    }

    bool all = safeURIs.empty();
    if (!all) {
        return;
    }

    // Plan to erase all files in this folder. Find any unaccounted for.

    const auto& auxURIs = storeState.dataAuxiliaryURIs();
    std::vector<eckit::PathName> allPathsVector;
    StdDir(basePath()).children(allPathsVector);
    for (const eckit::PathName& path : allPathsVector) {
        eckit::URI u("file", path);
        if (dataURIs.find(u) == dataURIs.end() && auxURIs.find(u) == auxURIs.end() &&
            safeURIs.find(u) == safeURIs.end()) {
            storeState.insertUnrecognised(u);
        }
    }
}

bool TocStore::doWipeUnknownContents(const std::set<eckit::URI>& unknownURIs) const {
    for (const auto& uri : unknownURIs) {
        if (uri.path().exists()) {
            remove(uri, std::cout, std::cout, true);
        }
    }

    return true;
}

bool TocStore::doWipe(const StoreWipeState& wipeState) const {

    bool wipeall = wipeState.safeURIs().empty();

    for (const auto& uri : wipeState.dataAuxiliaryURIs()) {
        remove(uri, std::cout, std::cout, true);
    }

    for (const auto& uri : wipeState.includedDataURIs()) {
        if (wipeall) {
            emptyDatabases_.emplace(uri.scheme(), uri.path().dirName());
        }
        remove(uri, std::cout, std::cout, true);
    }

    return true;
}

void TocStore::doWipeEmptyDatabases() const {

    for (const auto& uri : emptyDatabases_) {
        eckit::PathName path = uri.path();
        if (path.exists()) {
            remove(uri, std::cout, std::cout, true);
        }
    }

    emptyDatabases_.clear();
}

void TocStore::print(std::ostream& out) const {
    out << "TocStore(" << directory_ << ")";
}

static StoreBuilder<TocStore> builder("file");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

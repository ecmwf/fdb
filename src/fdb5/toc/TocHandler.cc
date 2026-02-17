/*
 i (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <fcntl.h>
#include <pwd.h>
#include <sys/types.h>
#include <cstddef>
#include <utility>

#include "eckit/config/Resource.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/FileDescHandle.h"
#include "eckit/io/FileHandle.h"
#include "eckit/log/BigNum.h"
#include "eckit/log/Log.h"
#include "eckit/maths/Functions.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/StaticMutex.h"
#include "eckit/utils/Literals.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/database/Index.h"
#include "fdb5/io/LustreSettings.h"
#include "fdb5/toc/TocCommon.h"
#include "fdb5/toc/TocFieldLocation.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/TocStats.h"

#if eckit_HAVE_AIO
#include <aio.h>
#endif

using namespace eckit;
using namespace eckit::literals;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {
constexpr const char* retrieve_lock_file    = "retrieve.lock";
constexpr const char* archive_lock_file     = "archive.lock";
constexpr const char* list_lock_file        = "list.lock";
constexpr const char* wipe_lock_file        = "wipe.lock";
constexpr const char* allow_duplicates_file = "duplicates.allow";
}  // namespace

const std::map<ControlIdentifier, const char*> controlfile_lookup{
    {ControlIdentifier::Retrieve, retrieve_lock_file},
    {ControlIdentifier::Archive, archive_lock_file},
    {ControlIdentifier::List, list_lock_file},
    {ControlIdentifier::Wipe, wipe_lock_file},
    {ControlIdentifier::UniqueRoot, allow_duplicates_file}};

//----------------------------------------------------------------------------------------------------------------------

class TocHandlerCloser {
    const TocHandler& handler_;

public:

    TocHandlerCloser(const TocHandler& handler) : handler_(handler) {}
    ~TocHandlerCloser() { handler_.close(); }
};

//----------------------------------------------------------------------------------------------------------------------

class CachedFDProxy {
public:  // methods

    CachedFDProxy(const eckit::PathName& path, int fd, std::unique_ptr<eckit::MemoryHandle>& cached) :
        path_(path), fd_(fd), cached_(cached.get()) {
        ASSERT((fd != -1) != (!!cached));
    }

    long read(void* buf, long len, const char** pdata = nullptr) {
        if (pdata && !cached_)
            throw SeriousBug("Can only return a pointer to data in memory if cached", Here());
        if (cached_) {
            if (pdata) {
                *pdata = reinterpret_cast<const char*>(cached_->data()) + cached_->position();
            }
            return cached_->read(buf, len);
        }
        else {
            long ret;
            SYSCALL2(ret = ::read(fd_, buf, len), path_);
            return ret;
        }
    }

    Offset position() {
        if (cached_) {
            return cached_->position();
        }
        else {
            off_t pos;
            SYSCALL(pos = ::lseek(fd_, 0, SEEK_CUR));
            return pos;
        }
    }

    Offset seek(const Offset& pos) {
        if (cached_) {
            return cached_->seek(pos);
        }
        else {
            off_t ret;
            SYSCALL(ret = ::lseek(fd_, pos, SEEK_SET));
            ASSERT(ret == static_cast<off_t>(pos));
            return pos;
        }
    }

private:  // members

    const eckit::PathName& path_;
    int fd_;
    MemoryHandle* cached_;
};

//----------------------------------------------------------------------------------------------------------------------

TocHandler::TocHandler(const eckit::PathName& directory, const Config& config) :
    TocCommon(directory),
    tocPath_(directory_ / "toc"),
    dbConfig_(config),
    serialisationVersion_(TocSerialisationVersion(config)),
    useSubToc_(config.userConfig().getBool("useSubToc", false)),
    isSubToc_(false),
    preloadBTree_(config.userConfig().getBool("preloadTocBTree", true)),
    fd_(-1),
    cachedToc_(nullptr),
    subTocRead_(nullptr),
    count_(0),
    enumeratedMaskedEntries_(false),
    numSubtocsRaw_(0),
    writeMode_(false),
    dirty_(false) {
    // An override to enable using sub tocs without configurations being passed in, for ease
    // of debugging
    const char* subTocOverride = ::getenv("FDB5_SUB_TOCS");
    if (subTocOverride) {
        useSubToc_ = true;
    }
}

TocHandler::TocHandler(const eckit::PathName& path, const Key& parentKey, MemoryHandle* cachedToc) :
    TocCommon(path.dirName()),
    parentKey_(parentKey),
    tocPath_(TocCommon::findRealPath(eckit::LocalPathName{path})),
    serialisationVersion_(TocSerialisationVersion(dbConfig_)),
    useSubToc_(false),
    isSubToc_(true),
    preloadBTree_(false),
    fd_(-1),
    cachedToc_(cachedToc),
    subTocRead_(nullptr),
    count_(0),
    enumeratedMaskedEntries_(false),
    numSubtocsRaw_(0),
    writeMode_(false),
    dirty_(false) {

    if (cachedToc_) {
        cachedToc_->openForRead();
    }

    /// Are we remapping a mounted DB?
    if (exists()) {
        Key key(databaseKey());
        if (!parentKey.empty() && parentKey != key) {

            LOG_DEBUG_LIB(LibFdb5) << "Opening (remapped) toc with differing parent key: " << key << " --> "
                                   << parentKey << std::endl;

            if (parentKey.size() != key.size()) {
                std::ostringstream ss;
                ss << "Keys insufficiently matching for mount: " << key << " : " << parentKey;
                throw UserError(ss.str(), Here());
            }

            for (const auto& kv : parentKey) {
                const auto [it, found] = key.find(kv.first);

                if (!found) {
                    std::ostringstream ss;
                    ss << "Keys insufficiently matching for mount: " << key << " : " << parentKey;
                    throw UserError(ss.str(), Here());
                }

                if (kv.second != it->second) {
                    remapKey_.set(kv.first, kv.second);
                }
            }

            LOG_DEBUG_LIB(LibFdb5) << "Key remapping: " << remapKey_ << std::endl;
        }
    }
}


TocHandler::~TocHandler() {
    close();
}

bool TocHandler::exists() const {
    return tocPath_.exists();
}

void TocHandler::openForAppend() {

    checkUID();  // n.b. may openForRead

    if (cachedToc_) {
        cachedToc_.reset();
    }

    writeMode_ = true;

    ASSERT(fd_ == -1);

    LOG_DEBUG_LIB(LibFdb5) << "Opening for append TOC " << tocPath_ << std::endl;

    int iomode = O_WRONLY | O_APPEND;
#ifdef O_NOATIME
    // this introduces issues of permissions
    static bool fdbNoATime = eckit::Resource<bool>("fdbNoATime;$FDB_OPEN_NOATIME", false);
    if (fdbNoATime) {
        iomode |= O_NOATIME;
    }
#endif
    SYSCALL2((fd_ = ::open(tocPath_.localPath(), iomode, (mode_t)0777)), tocPath_);
}

void TocHandler::openForRead() const {

    if (cachedToc_) {
        ASSERT(not writeMode_);
        cachedToc_->seek(0);
        return;
    }

    static bool fdbCacheTocsOnRead = eckit::Resource<bool>("fdbCacheTocsOnRead;$FDB_CACHE_TOCS_ON_READ", true);

    ASSERT(fd_ == -1);

    writeMode_ = false;

    LOG_DEBUG_LIB(LibFdb5) << "Opening for read TOC " << tocPath_ << std::endl;

    int iomode = O_RDONLY;
#ifdef O_NOATIME
    // this introduces issues of permissions
    static bool fdbNoATime = eckit::Resource<bool>("fdbNoATime;$FDB_OPEN_NOATIME", false);
    if (fdbNoATime) {
        iomode |= O_NOATIME;
    }
#endif
    SYSCALL2((fd_ = ::open(tocPath_.localPath(), iomode)), tocPath_);
    eckit::Length tocSize = tocPath_.size();

    // The masked subtocs and indexes could be updated each time, so reset this.
    enumeratedMaskedEntries_ = false;
    numSubtocsRaw_           = 0;
    maskedEntries_.clear();

    if (fdbCacheTocsOnRead) {

        FileDescHandle toc(fd_, true);  // closes the file descriptor
        AutoClose closer1(toc);
        fd_ = -1;


        bool grow = true;
        cachedToc_.reset(new eckit::MemoryHandle(tocSize, grow));

        long buffersize = 4_MiB;
        toc.copyTo(*cachedToc_, buffersize, tocSize, tocReadStats_);
        cachedToc_->openForRead();
    }
}

void TocHandler::dumpTocCache() const {
    if (cachedToc_) {
        eckit::Offset offset = cachedToc_->position();
        cachedToc_->seek(0);

        eckit::PathName tocDumpFile("dump_of_" + tocPath_.baseName());
        eckit::FileHandle dump(eckit::PathName::unique(tocDumpFile));
        cachedToc_->copyTo(dump);

        std::ostringstream ss;
        ss << tocPath_.baseName() << " read in " << tocReadStats_.size() << " step"
           << ((tocReadStats_.size() > 1) ? "s" : "") << std::endl;
        double time;
        eckit::Length len;
        while (tocReadStats_.next(time, len)) {
            ss << "  step duration: " << (time * 1000) << " ms, size: " << len << " bytes" << std::endl;
        }
        Log::error() << ss.str();

        cachedToc_->seek(offset);
    }
}

void TocHandler::append(TocRecord& r, size_t payloadSize) {

    LOG_DEBUG_LIB(LibFdb5) << "Writing toc entry: " << (int)r.header_.tag_ << std::endl;

    appendRound(r, payloadSize);
}

void TocHandler::appendRound(TocRecord& r, size_t payloadSize) {
    // Obtain the rounded size, and set it in the record header.
    auto [realSize, roundedSize] = recordSizes(r, payloadSize);

    eckit::Buffer buf(roundedSize);
    buf.zero();
    buf.copy(static_cast<const void*>(&r), realSize);

    appendRaw(buf, buf.size());
}

void TocHandler::appendRaw(const void* data, size_t size) {

    ASSERT(fd_ != -1);
    ASSERT(not cachedToc_);

    ASSERT(size % recordRoundSize() == 0);

    size_t len;
    SYSCALL2(len = ::write(fd_, data, size), tocPath_);
    dirty_ = true;
    ASSERT(len == size);
}

void TocHandler::appendBlock(TocRecord& r, size_t payloadSize) {

    openForAppend();
    TocHandlerCloser close(*this);

    appendRound(r, payloadSize);
}

void TocHandler::appendBlock(const void* data, size_t size) {

    openForAppend();
    TocHandlerCloser close(*this);

    appendRaw(data, size);
}

const TocSerialisationVersion& TocHandler::serialisationVersion() const {
    return serialisationVersion_;
}

size_t TocHandler::recordRoundSize() {

    static size_t fdbRoundTocRecords = eckit::Resource<size_t>("fdbRoundTocRecords", 1024);
    return fdbRoundTocRecords;
}

std::pair<size_t, size_t> TocHandler::recordSizes(TocRecord& r, size_t payloadSize) {

    size_t dataSize = sizeof(TocRecord::Header) + payloadSize;
    r.header_.size_ = eckit::round(dataSize, recordRoundSize());

    return {dataSize, r.header_.size_};
}

bool TocHandler::ignoreIndex(const TocRecord& r, bool readMasked) const {
    ASSERT(r.header_.tag_ == TocRecord::TOC_INDEX);

    eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
    eckit::LocalPathName path;
    off_t offset;
    s >> path;
    s >> offset;

    /// @note: currentDirectory() may return the active subtoc's directory
    LocalPathName absPath = currentDirectory() / path;

    std::pair<LocalPathName, size_t> key(absPath.baseName(), offset);
    if (maskedEntries_.find(key) != maskedEntries_.end()) {
        if (!readMasked) {
            LOG_DEBUG_LIB(LibFdb5) << "Index ignored by mask: " << path << ":" << offset << std::endl;
            return true;
        }
        // This is a masked index, so it is valid for it to not exist.
        if (!absPath.exists()) {
            LOG_DEBUG_LIB(LibFdb5) << "Index does not exist: " << path << ":" << offset << std::endl;
            return true;
        }
    }

    return false;
}

// readNext wraps readNextInternal.
// readNext reads the next TOC entry from this toc, or from an appropriate subtoc if necessary.
bool TocHandler::readNext(TocRecord& r, bool walkSubTocs, bool hideSubTocEntries, bool hideClearEntries,
                          bool readMasked, const TocRecord** data, size_t* length, LocalPathName* parentTocPath) const {

    int len;

    // For some tools (mainly diagnostic) it makes sense to be able to switch the
    // walking behaviour here.

    if (!walkSubTocs)
        return readNextInternal(r, data, length);

    // Ensure we are able to skip masked entries as appropriate

    if (!enumeratedMaskedEntries_) {
        populateMaskedEntriesList();
        preloadSubTocs(readMasked);
    }

    while (true) {
        if (subTocRead_) {
            len = subTocRead_->readNext(r, walkSubTocs, hideSubTocEntries, hideClearEntries, readMasked, data, length);
            if (len == 0) {
                subTocRead_ = nullptr;
                if (parentTocPath) {
                    *parentTocPath = "";
                }
            }
            else if (r.header_.tag_ == TocRecord::TOC_INDEX) {
                // Check if a TOC_CLEAR in this toc is masking the subtoc index
                if (ignoreIndex(r, readMasked)) {
                    continue;
                }
                return true;
            }
            else {
                ASSERT(r.header_.tag_ != TocRecord::TOC_SUB_TOC);
                return true;
            }
        }
        else {

            if (!readNextInternal(r, data, length)) {

                return false;
            }
            else if (r.header_.tag_ == TocRecord::TOC_INIT) {

                eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
                if (parentKey_.empty())
                    parentKey_ = Key(s);
                return true;
            }
            else if (r.header_.tag_ == TocRecord::TOC_SUB_TOC) {

                LocalPathName absPath = parseSubTocRecord(r, readMasked);
                if (absPath == "")
                    continue;

                if (parentTocPath) {
                    *parentTocPath = currentTocPath();
                }
                selectSubTocRead(absPath);

                if (hideSubTocEntries) {
                    // The first entry in a subtoc must be the init record. Check that
                    subTocRead_->readNext(r, walkSubTocs, hideSubTocEntries, hideClearEntries, readMasked, data,
                                          length);
                    ASSERT(r.header_.tag_ == TocRecord::TOC_INIT);
                }
                else {
                    return true;  // if not hiding the subtoc entries, return them as normal entries!
                }
            }
            else if (r.header_.tag_ == TocRecord::TOC_INDEX) {

                if (ignoreIndex(r, readMasked)) {
                    continue;
                }
                return true;
            }
            else if (r.header_.tag_ == TocRecord::TOC_CLEAR && hideClearEntries) {
                continue;  // we already handled the TOC_CLEAR entries in populateMaskedEntriesList()
            }
            else {
                // A normal read operation
                return true;
            }
        }
    }
}

// readNext wraps readNextInternal.
// readNextInternal reads the next TOC entry from this toc.
bool TocHandler::readNextInternal(TocRecord& r, const TocRecord** data, size_t* length) const {

    CachedFDProxy proxy(tocPath_, fd_, cachedToc_);

    try {
        long len = proxy.read(&r, sizeof(TocRecord::Header), reinterpret_cast<const char**>(data));
        if (len == 0) {
            return false;
        }
        ASSERT(len == sizeof(TocRecord::Header));
    }
    catch (...) {
        dumpTocCache();
        throw;
    }

    try {
        long len = proxy.read(&r.payload_, r.header_.size_ - sizeof(TocRecord::Header));
        ASSERT(size_t(len) == r.header_.size_ - sizeof(TocRecord::Header));

        if (length)
            (*length) = len + sizeof(TocRecord::Header);
    }
    catch (...) {
        dumpTocCache();
        throw;
    }

    serialisationVersion_.check(r.header_.serialisationVersion_, true);

    return true;
}

std::vector<PathName> TocHandler::subTocPaths() const {

    openForRead();
    TocHandlerCloser close(*this);

    auto r = std::make_unique<TocRecord>(
        serialisationVersion_.used());  // allocate (large) TocRecord on heap not stack (MARS-779)

    std::vector<eckit::PathName> paths;

    bool walkSubTocs       = true;
    bool hideSubTocEntries = false;
    bool hideClearEntries  = true;
    while (readNext(*r, walkSubTocs, hideSubTocEntries, hideClearEntries)) {

        eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);
        std::string path;

        switch (r->header_.tag_) {

            case TocRecord::TOC_SUB_TOC: {
                // n.b. don't just push path onto paths, as it may be relative to a subtoc
                //      in a different DB (e.g. through an overlay). So query it properly.
                ASSERT(subTocRead_);
                paths.push_back(currentTocPath());
                break;
            }

            case TocRecord::TOC_CLEAR:
                ASSERT_MSG(r->header_.tag_ != TocRecord::TOC_CLEAR,
                           "The TOC_CLEAR records should have been pre-filtered on the first pass");
                break;

            case TocRecord::TOC_INIT:
                break;

            case TocRecord::TOC_INDEX:
                break;

            default: {
                // This is only a warning, as it is legal for later versions of software to add stuff
                // that is just meaningless in a backwards-compatible sense.
                Log::warning() << "Unknown TOC entry " << (*r) << " @ " << Here() << std::endl;
                break;
            }
        }
    }

    return paths;
}

void TocHandler::close() const {

    if (subTocRead_) {
        subTocRead_->close();
        subTocRead_ = 0;
    }

    if (subTocWrite_) {
        // We keep track of the sub toc we are writing to until the process is closed, so don't reset
        // the pointer here (or we will create a proliferation of sub tocs)
        subTocWrite_->close();
    }

    if (fd_ >= 0) {
        LOG_DEBUG_LIB(LibFdb5) << "Closing TOC " << tocPath_ << std::endl;
        if (dirty_) {
            SYSCALL2(eckit::fdatasync(fd_), tocPath_);
            dirty_ = false;
        }
        SYSCALL2(::close(fd_), tocPath_);
        fd_        = -1;
        writeMode_ = false;
    }
}

void TocHandler::allMaskableEntries(Offset startOffset, Offset endOffset,
                                    std::set<std::pair<LocalPathName, Offset>>& maskedEntries) const {

    CachedFDProxy proxy(tocPath_, fd_, cachedToc_);

    // Start reading entries where we are told to

    Offset ret = proxy.seek(startOffset);
    ASSERT(ret == startOffset);

    auto r = std::make_unique<TocRecord>(
        serialisationVersion_.used());  // allocate (large) TocRecord on heap not stack (MARS-779)

    while (proxy.position() < endOffset) {

        ASSERT(readNextInternal(*r));

        eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);
        LocalPathName path;
        off_t offset;

        switch (r->header_.tag_) {
            case TocRecord::TOC_SUB_TOC: {
                s >> path;
                maskedEntries.emplace(std::pair<PathName, Offset>(path.baseName(), 0));
                break;
            }

            case TocRecord::TOC_INDEX:
                s >> path;
                s >> offset;
                // readNextInternal --> use directory_ not currentDirectory()
                maskedEntries.emplace(std::pair<PathName, Offset>(path, offset));
                break;

            case TocRecord::TOC_CLEAR:
                break;
            case TocRecord::TOC_INIT:
                break;

            default: {
                // This is only a warning, as it is legal for later versions of software to add stuff
                // that is just meaningless in a backwards-compatible sense.
                Log::warning() << "Unknown TOC entry " << (*r) << " @ " << Here() << std::endl;
                break;
            }
        }
    }
}

eckit::LocalPathName TocHandler::parseSubTocRecord(const TocRecord& r, bool readMasked) const {

    eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
    eckit::LocalPathName path;
    s >> path;
    // Handle both path and absPath for compatibility as we move from storing
    // absolute paths to relative paths. Either may exist in either the TOC_SUB_TOC
    // or TOC_CLEAR entries.
    ASSERT(path.path().size() > 0);
    LocalPathName absPath;
    if (path.path()[0] == '/') {
        absPath = findRealPath(path);
        if (!absPath.exists()) {
            absPath = currentDirectory() / path.baseName();
        }
    }
    else {
        absPath = currentDirectory() / path;
    }

    // If this subtoc has a masking entry, then skip it, and go on to the next entry.
    // Unless readMasked is true, in which case walk it if it exists.
    std::pair<eckit::LocalPathName, size_t> key(absPath.baseName(), 0);
    if (maskedEntries_.find(key) != maskedEntries_.end()) {
        if (!readMasked) {
            LOG_DEBUG_LIB(LibFdb5) << "SubToc ignored by mask: " << path << std::endl;
            return "";
        }
        // This is a masked subtoc, so it is valid for it to not exist.
        if (!absPath.exists()) {
            LOG_DEBUG_LIB(LibFdb5) << "SubToc does not exist: " << path << std::endl;
            return "";
        }
    }

    LOG_DEBUG_LIB(LibFdb5) << "Opening SUB_TOC: " << absPath << " " << parentKey_ << std::endl;

    return absPath;
}

class SubtocPreloader {

    struct AutoFDCloser {
        int fd_;
        AutoFDCloser(int fd) : fd_(fd) {}
        AutoFDCloser(AutoFDCloser&& rhs) : fd_(rhs.fd_) { rhs.fd_ = -1; }
        AutoFDCloser(const AutoFDCloser&) = delete;
        AutoFDCloser& operator=(AutoFDCloser&& rhs) {
            fd_     = rhs.fd_;
            rhs.fd_ = -1;
            return *this;
        }
        AutoFDCloser& operator=(const AutoFDCloser&) = delete;
        ~AutoFDCloser() {
            if (fd_ > 0)
                ::close(fd_);  // n.b. ignore return value
        }
    };

    const Key& parentKey_;

    mutable std::map<eckit::LocalPathName, std::unique_ptr<TocHandler>> subTocReadCache_;
    std::vector<eckit::LocalPathName> paths_;

public:

    explicit SubtocPreloader(const Key& parentKey) : parentKey_(parentKey) {}

    decltype(subTocReadCache_)&& cache() {

#if eckit_HAVE_AIO
        int iomode = O_RDONLY;  // | O_DIRECT;
#ifdef O_NOATIME
        // this introduces issues of permissions
        static bool fdbNoATime = eckit::Resource<bool>("fdbNoATime;$FDB_OPEN_NOATIME", false);
        if (fdbNoATime) {
            iomode |= O_NOATIME;
        }
#endif

        std::vector<aiocb> aiocbs(paths_.size());
        std::vector<Buffer> buffers(paths_.size());
        std::vector<AutoFDCloser> closers;
        std::vector<char> done(paths_.size());
        ::memset(done.data(), 0, done.size() * sizeof(char));
        ::memset(aiocbs.data(), 0, sizeof(aiocb) * aiocbs.size());

        {
            eckit::Timer sstime("subtocs.statsubmit", Log::debug<LibFdb5>());
            for (int i = 0; i < aiocbs.size(); ++i) {

                const eckit::LocalPathName& path = paths_[i];

                int fd;
                SYSCALL2((fd = ::open(path.localPath(), iomode)), path);
                closers.emplace_back(AutoFDCloser{fd});
                eckit::Length tocSize = path.size();

                aiocb& aio(aiocbs[i]);
                zero(aio);

                aio.aio_fildes                = fd;
                aio.aio_offset                = 0;
                aio.aio_nbytes                = tocSize;
                aio.aio_sigevent.sigev_notify = SIGEV_NONE;

                buffers[i].resize(tocSize);
                aio.aio_buf = buffers[i].data();

                SYSCALL(::aio_read(&aio));
            }
        }

        std::vector<aiocb*> aiocbPtrs(aiocbs.size());
        for (int i = 0; i < aiocbs.size(); ++i) {
            aiocbPtrs[i] = &aiocbs[i];
        }

        int doneCount = 0;

        {
            eckit::Timer sstime("subtocs.collect", Log::debug<LibFdb5>());

            while (doneCount < aiocbs.size()) {

                // Now wait until data is ready from at least one read

                errno = 0;
                while (::aio_suspend(aiocbPtrs.data(), aiocbs.size(), nullptr) < 0) {
                    if (errno != EINTR) {
                        throw FailedSystemCall("aio_suspend", Here(), errno);
                    }
                }

                // Find which one(s) are ready

                for (int n = 0; n < aiocbs.size(); ++n) {

                    if (done[n])
                        continue;

                    int e = ::aio_error(&aiocbs[n]);
                    if (e == EINPROGRESS) {
                        continue;
                    }

                    if (e == 0) {

                        ssize_t len = ::aio_return(&aiocbs[n]);
                        if (len != buffers[n].size()) {
                            aiocbs[n].aio_nbytes = len;
                        }

                        bool grow      = true;
                        auto cachedToc = std::make_unique<eckit::MemoryHandle>(buffers[n].size(), grow);

                        {
                            cachedToc->openForWrite(aiocbs[n].aio_nbytes);
                            AutoClose closer(*cachedToc);
                            ASSERT(cachedToc->write(buffers[n].data(), aiocbs[n].aio_nbytes) == aiocbs[n].aio_nbytes);
                        }
                        ASSERT(subTocReadCache_.find(paths_[n]) == subTocReadCache_.end());
                        subTocReadCache_.emplace(
                            paths_[n], std::make_unique<TocHandler>(paths_[n], parentKey_, cachedToc.release()));

                        done[n] = true;
                        doneCount++;
                    }
                    else {
                        throw FailedSystemCall("aio_error", Here(), e);
                    }
                }
            }
        }
#else
        NOTIMP;
#endif  // eckit_HAVE_AIO
        return std::move(subTocReadCache_);
    }

    void addPath(const eckit::LocalPathName& path) { paths_.push_back(path); }
};

void TocHandler::preloadSubTocs(bool readMasked) const {
    ASSERT(enumeratedMaskedEntries_);
    if (numSubtocsRaw_ == 0)
        return;

    CachedFDProxy proxy(tocPath_, fd_, cachedToc_);
    Offset startPosition = proxy.position();  // remember the current position of the file descriptor

    subTocReadCache_.clear();

    eckit::Timer preloadTimer("subtocs.preload", Log::debug<LibFdb5>());
    {
        auto r = std::make_unique<TocRecord>(
            serialisationVersion_.used());  // allocate (large) TocRecord on heap not stack (MARS-779)

        // n.b. we call databaseKey() directly, as this preload will normally be called before we have walked
        //      the toc at all --> TOC_INIT not yet read --> parentKey_ not yet set.
        SubtocPreloader preloader(parentKey_);

        while (readNextInternal(*r)) {

            switch (r->header_.tag_) {
                case TocRecord::TOC_SUB_TOC: {
                    LocalPathName absPath = parseSubTocRecord(*r, readMasked);
                    if (absPath != "")
                        preloader.addPath(absPath);
                } break;
                case TocRecord::TOC_INIT:
                    break;
                case TocRecord::TOC_INDEX:
                    break;
                case TocRecord::TOC_CLEAR:
                    break;
                default: {
                    // This is only a warning, as it is legal for later versions of software to add stuff
                    // that is just meaningless in a backwards-compatible sense.
                    Log::warning() << "Unknown TOC entry " << (*r) << " @ " << Here() << std::endl;
                    break;
                }
            }
        }

        Offset ret = proxy.seek(startPosition);
        ASSERT(ret == startPosition);

        subTocReadCache_ = std::move(preloader.cache());
    }
    preloadTimer.stop();
}

void TocHandler::populateMaskedEntriesList() const {

    ASSERT(fd_ != -1 || cachedToc_);
    CachedFDProxy proxy(tocPath_, fd_, cachedToc_);

    Offset startPosition = proxy.position();  // remember the current position of the file descriptor

    maskedEntries_.clear();

    auto r = std::make_unique<TocRecord>(
        serialisationVersion_.used());  // allocate (large) TocRecord on heap not stack (MARS-779)

    size_t countSubTocs = 0;

    while (readNextInternal(*r)) {

        eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);
        std::string path;
        off_t offset;

        switch (r->header_.tag_) {

            case TocRecord::TOC_CLEAR: {
                s >> path;
                s >> offset;

                if (path == "*") {  // For the "*" path, mask EVERYTHING that we have already seen
                    Offset currentPosition = proxy.position();
                    allMaskableEntries(startPosition, currentPosition, maskedEntries_);
                    ASSERT(currentPosition == proxy.position());
                }
                else {
                    // readNextInternal --> use directory_ not currentDirectory()
                    // ASSERT(path.size() > 0);
                    // eckit::PathName absPath = (path[0] == '/') ? findRealPath(path) : (directory_ / path);
                    eckit::PathName pathName = path;
                    maskedEntries_.emplace(std::pair<PathName, Offset>(pathName.baseName(), offset));
                }
                break;
            }

            case TocRecord::TOC_SUB_TOC:
                countSubTocs++;
                break;
            case TocRecord::TOC_INIT:
                break;
            case TocRecord::TOC_INDEX:
                break;

            default: {
                // This is only a warning, as it is legal for later versions of software to add stuff
                // that is just meaningless in a backwards-compatible sense.
                Log::warning() << "Unknown TOC entry " << (*r) << " @ " << Here() << std::endl;
                break;
            }
        }
    }

    // And reset back to where we were.

    Offset ret = proxy.seek(startPosition);
    ASSERT(ret == startPosition);

    numSubtocsRaw_           = countSubTocs;
    enumeratedMaskedEntries_ = true;
}

static eckit::StaticMutex local_mutex;

void TocHandler::writeInitRecord(const Key& key) {

    eckit::AutoLock<eckit::StaticMutex> lock(local_mutex);

    if (!directory_.exists()) {
        directory_.mkdir();
    }

    // enforce lustre striping if requested
    if (stripeLustre() && !tocPath_.exists()) {
        fdb5LustreapiFileCreate(tocPath_, stripeIndexLustreSettings());
    }

    ASSERT(fd_ == -1);

    int iomode = O_CREAT | O_RDWR;
    SYSCALL2(fd_ = ::open(tocPath_.localPath(), iomode, mode_t(0777)), tocPath_);

    TocHandlerCloser closer(*this);

    auto r = std::make_unique<TocRecord>(
        serialisationVersion_.used());  // allocate (large) TocRecord on heap not stack (MARS-779)

    size_t len = readNext(*r);
    if (len == 0) {

        LOG_DEBUG_LIB(LibFdb5) << "Initializing FDB TOC in " << tocPath_ << std::endl;

        if (!isSubToc_) {

            /* Copy schema first */

            LOG_DEBUG_LIB(LibFdb5) << "Copy schema from " << dbConfig_.schemaPath() << " to " << schemaPath_
                                   << std::endl;

            eckit::LocalPathName tmp{eckit::PathName::unique(schemaPath_)};

            eckit::FileHandle in(dbConfig_.schemaPath());

            // Enforce lustre striping if requested

            // SDS: Would be nicer to do this, but FileHandle doesn't have a path_ member, let alone an exposed one
            //      so would need some tinkering to work with LustreFileHandle.
            // LustreFileHandle<eckit::FileHandle> out(tmp, stripeIndexLustreSettings());

            if (stripeLustre()) {
                fdb5LustreapiFileCreate(tmp, stripeIndexLustreSettings());
            }
            eckit::FileHandle out(tmp);
            in.copyTo(out);

            eckit::LocalPathName::rename(tmp, schemaPath_);
        }

        auto r2 = std::make_unique<TocRecord>(
            serialisationVersion_.used(),
            TocRecord::TOC_INIT);  // allocate (large) TocRecord on heap not stack (MARS-779)
        eckit::MemoryStream s(&r2->payload_[0], r2->maxPayloadSize);
        s << key;
        s << isSubToc_;
        append(*r2, s.position());
        dbUID_ = r2->header_.uid_;
    }
    else {
        ASSERT(r->header_.tag_ == TocRecord::TOC_INIT);
        eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);
        ASSERT(key == Key(s));
        dbUID_ = r->header_.uid_;
    }
}

void TocHandler::writeClearRecord(const Index& index) {

    auto r = std::make_unique<TocRecord>(
        serialisationVersion_.used(), TocRecord::TOC_CLEAR);  // allocate (large) TocRecord on heap not stack (MARS-779)

    appendBlock(*r, buildClearRecord(*r, index));
}

void TocHandler::writeClearAllRecord() {

    auto r = std::make_unique<TocRecord>(
        serialisationVersion_.used(), TocRecord::TOC_CLEAR);  // allocate (large) TocRecord on heap not stack (MARS-779)

    eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);
    s << std::string{"*"};
    s << off_t{0};

    appendBlock(*r, s.position());
}


void TocHandler::writeSubTocRecord(const TocHandler& subToc) {

    openForAppend();
    TocHandlerCloser closer(*this);

    auto r =
        std::make_unique<TocRecord>(serialisationVersion_.used(),
                                    TocRecord::TOC_SUB_TOC);  // allocate (large) TocRecord on heap not stack (MARS-779)

    eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);

    // We use a relative path to this subtoc if it belongs to the current DB
    // but an absolute one otherwise (e.g. for fdb-overlay).
    const PathName& absPath = subToc.tocPath();
    eckit::PathName path    = (absPath.dirName().sameAs(directory_)) ? absPath.baseName() : absPath;

    s << path;
    s << off_t{0};
    append(*r, s.position());

    LOG_DEBUG_LIB(LibFdb5) << "Write TOC_SUB_TOC " << path << std::endl;
}


void TocHandler::writeIndexRecord(const Index& index) {

    // Toc index writer

    struct WriteToStream : public IndexLocationVisitor {
        WriteToStream(const Index& index, TocHandler& handler) : index_(index), handler_(handler) {}

        virtual void operator()(const IndexLocation& l) {

            const TocIndexLocation& location = reinterpret_cast<const TocIndexLocation&>(l);

            auto r = std::make_unique<TocRecord>(
                handler_.serialisationVersion_.used(),
                TocRecord::TOC_INDEX);  // allocate (large) TocRecord on heap not stack (MARS-779)

            eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);

            s << location.uri().path().baseName();
            s << location.offset();
            s << index_.type();

            index_.encode(s, r->header_.serialisationVersion_);
            handler_.append(*r, s.position());

            LOG_DEBUG_LIB(LibFdb5) << "Write TOC_INDEX " << location.uri().path().baseName() << " - "
                                   << location.offset() << " " << index_.type() << std::endl;
        }

    private:

        const Index& index_;
        TocHandler& handler_;
    };

    // If we are using a sub toc, delegate there

    if (useSubToc_) {

        // Create the sub toc, and insert the redirection record into the the master toc.

        if (!subTocWrite_) {

            eckit::PathName subtoc = eckit::PathName::unique("toc");

            subTocWrite_.reset(new TocHandler(currentDirectory() / subtoc, Key{}));

            subTocWrite_->writeInitRecord(databaseKey());

            writeSubTocRecord(*subTocWrite_);
        }

        subTocWrite_->writeIndexRecord(index);
        return;
    }

    // Otherwise, we actually do the writing!

    openForAppend();
    TocHandlerCloser closer(*this);

    WriteToStream writeVisitor(index, *this);
    index.visit(writeVisitor);
}

void TocHandler::writeSubTocMaskRecord(const TocHandler& subToc) {

    auto r = std::make_unique<TocRecord>(
        serialisationVersion_.used(), TocRecord::TOC_CLEAR);  // allocate (large) TocRecord on heap not stack (MARS-779)

    // We use a relative path to this subtoc if it belongs to the current DB
    // but an absolute one otherwise (e.g. for fdb-overlay).
    const PathName& absPath = subToc.tocPath();
    PathName path           = (absPath.dirName().sameAs(directory_)) ? absPath.baseName() : absPath;

    appendBlock(*r, buildSubTocMaskRecord(*r, path));
}

bool TocHandler::useSubToc() const {
    return useSubToc_;
}

bool TocHandler::anythingWrittenToSubToc() const {
    return !!subTocWrite_;
}

//----------------------------------------------------------------------------------------------------------------------

class HasPath {

    eckit::PathName path_;
    off_t offset_;

public:

    HasPath(const eckit::PathName& path, off_t offset) : path_(path), offset_(offset) {}
    bool operator()(const Index index) const {

        const TocIndex* tocidx = dynamic_cast<const TocIndex*>(index.content());

        if (!tocidx) {
            throw eckit::NotImplemented(
                "Index is not of TocIndex type -- referencing unknown Index types isn't supported", Here());
        }

        return (tocidx->path() == path_) && (tocidx->offset() == offset_);
    }
};

uid_t TocHandler::dbUID() const {

    if (dbUID_ != static_cast<uid_t>(-1)) {
        return dbUID_;
    }

    openForRead();
    TocHandlerCloser close(*this);

    auto r = std::make_unique<TocRecord>(
        serialisationVersion_.used());  // allocate (large) TocRecord on heap not stack (MARS-779)

    while (readNext(*r)) {
        if (r->header_.tag_ == TocRecord::TOC_INIT) {
            dbUID_ = r->header_.uid_;
            return dbUID_;
        }
    }

    throw eckit::SeriousBug("Cannot find a TOC_INIT record");
}

Key TocHandler::databaseKey() {
    openForRead();
    TocHandlerCloser close(*this);

    auto r = std::make_unique<TocRecord>(
        serialisationVersion_.used());  // allocate (large) TocRecord on heap not stack (MARS-779)

    bool walkSubTocs = false;
    while (readNext(*r, walkSubTocs)) {
        if (r->header_.tag_ == TocRecord::TOC_INIT) {
            eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);
            dbUID_ = r->header_.uid_;
            return Key(s);
        }
    }

    throw eckit::SeriousBug("Cannot find a TOC_INIT record");
}

size_t TocHandler::numberOfRecords() const {

    if (count_ == 0) {
        openForRead();
        TocHandlerCloser close(*this);

        auto r = std::make_unique<TocRecord>(
            serialisationVersion_.used());  // allocate (large) TocRecord on heap not stack (MARS-779)

        bool walkSubTocs       = true;
        bool hideSubTocEntries = false;
        bool hideClearEntries  = false;
        while (readNext(*r, walkSubTocs, hideSubTocEntries, hideClearEntries)) {
            count_++;
        }
    }

    return count_;
}

const eckit::LocalPathName& TocHandler::directory() const {
    return directory_;
}

std::vector<Index> TocHandler::loadIndexes(bool sorted, std::set<std::string>* subTocs,
                                           std::vector<bool>* indexInSubtoc, std::vector<Key>* remapKeys) const {

    std::vector<Index> indexes;

    if (!tocPath_.exists()) {
        return indexes;
    }

    // If we haven't yet read the TOC_INIT record to extract the parentKey, it may be needed for
    // subtoc handling...
    // We've got a bit mangled with our constness here...
    if (parentKey_.empty() && remapKeys && !isSubToc_) {
        const auto& k = const_cast<TocHandler&>(*this).databaseKey();
        parentKey_    = k;
    }

    openForRead();
    TocHandlerCloser close(*this);

    auto r = std::make_unique<TocRecord>(
        serialisationVersion_.used());  // allocate (large) TocRecord on heap not stack (MARS-779)
    count_ = 0;

    // A record of all the index entries found (to process later)
    struct IndexEntry {
        size_t seqNo;
        const TocRecord* datap;
        size_t dataLen;
        LocalPathName tocDirectoryName;  // May differ if using the overlay
    };
    std::vector<IndexEntry> indexEntries;

    bool debug             = LibFdb5::instance().debug();
    bool walkSubTocs       = true;
    bool hideSubTocEntries = true;
    bool hideClearEntries  = true;
    bool readMasked        = false;
    const TocRecord* pdata;
    size_t dataLength;
    while (readNext(*r, walkSubTocs, hideSubTocEntries, hideClearEntries, readMasked, &pdata, &dataLength)) {

        eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);
        std::string path;
        std::string type;

        off_t offset;
        std::vector<Index>::iterator j;

        count_++;


        switch (r->header_.tag_) {

            case TocRecord::TOC_INIT:
                dbUID_ = r->header_.uid_;
                LOG_DEBUG(debug, LibFdb5) << "TocRecord TOC_INIT key is " << Key(s) << std::endl;
                break;

            case TocRecord::TOC_INDEX:
                indexEntries.emplace_back(IndexEntry{indexEntries.size(), pdata, dataLength, currentDirectory()});

                if (subTocs && subTocRead_) {
                    subTocs->insert(subTocRead_->tocPath());
                }
                if (indexInSubtoc) {
                    indexInSubtoc->push_back(!!subTocRead_);
                }
                if (remapKeys) {
                    remapKeys->push_back(currentRemapKey());
                }
                break;

            case TocRecord::TOC_CLEAR:
                ASSERT_MSG(r->header_.tag_ != TocRecord::TOC_CLEAR,
                           "The TOC_CLEAR records should have been pre-filtered on the first pass");
                break;

            case TocRecord::TOC_SUB_TOC:
                throw eckit::SeriousBug("TOC_SUB_TOC entry should be handled inside readNext");

            default:
                std::ostringstream oss;
                oss << "Unknown tag in TocRecord " << *r;
                throw eckit::SeriousBug(oss.str(), Here());
        }
    }

    // Now construct the index objects (we can parallelise this...)
    // n.b. would be nicer to use std::for_each with a policy ... but that doesn't work for now.

    static const int nthreads = eckit::Resource<long>("fdbLoadIndexThreads;$FDB_LOAD_INDEX_THREADS", 1);

    {
        std::vector<std::future<void>> threads;
        std::vector<TocIndex*> tocindexes(indexEntries.size(), nullptr);

        for (int i = 0; i < nthreads; ++i) {
            threads.emplace_back(std::async(std::launch::async, [i, &indexEntries, &tocindexes, debug, this] {
                for (int idx = i; idx < indexEntries.size(); idx += nthreads) {

                    const IndexEntry& entry = indexEntries[idx];
                    eckit::MemoryStream s(entry.datap->payload_, entry.dataLen - sizeof(TocRecord::Header));
                    LocalPathName path;
                    off_t offset;
                    std::string type;
                    s >> path;
                    s >> offset;
                    s >> type;
                    LOG_DEBUG(debug, LibFdb5) << "TocRecord TOC_INDEX " << path << " - " << offset << std::endl;
                    tocindexes[entry.seqNo] =
                        new TocIndex(s, entry.datap->header_.serialisationVersion_, entry.tocDirectoryName,
                                     entry.tocDirectoryName / path, offset, preloadBTree_);
                }
            }));
        }

        for (auto& thread : threads)
            thread.get();

        indexes.reserve(indexEntries.size());
        for (TocIndex* ti : tocindexes) {
            indexes.emplace_back(ti);
        }
    }

    // For some purposes, it is useful to have the indexes sorted by their location, as this is is faster for
    // iterating through the data.

    if (sorted) {

        ASSERT(!indexInSubtoc);
        ASSERT(!remapKeys);
        std::sort(indexes.begin(), indexes.end(), TocIndexFileSort());
    }
    else {

        // In the normal case, the entries are sorted into reverse order. The last index takes precedence
        std::reverse(indexes.begin(), indexes.end());

        if (indexInSubtoc) {
            std::reverse(indexInSubtoc->begin(), indexInSubtoc->end());
        }
        if (remapKeys) {
            std::reverse(remapKeys->begin(), remapKeys->end());
        }
    }

    return indexes;
}

const eckit::LocalPathName& TocHandler::tocPath() const {
    return tocPath_;
}

const eckit::LocalPathName& TocHandler::schemaPath() const {
    return schemaPath_;
}

void TocHandler::selectSubTocRead(const eckit::LocalPathName& path) const {

    auto it = subTocReadCache_.find(path);
    if (it == subTocReadCache_.end()) {
        auto r = subTocReadCache_.insert(std::make_pair(path, new TocHandler(path, parentKey_)));
        ASSERT(r.second);
        it = r.first;
    }

    subTocRead_ = it->second.get();
    subTocRead_->openForRead();
}

void TocHandler::dump(std::ostream& out, bool simple, bool walkSubTocs, bool dumpStructure) const {

    openForRead();
    TocHandlerCloser close(*this);

    auto r = std::make_unique<TocRecord>(
        serialisationVersion_.used());  // allocate (large) TocRecord on heap not stack (MARS-779)

    bool hideSubTocEntries = false;
    bool hideClearEntries  = false;
    bool readMasked = dumpStructure;  // disabled by default, to get accurate file offsets we need to read masked data.

    LocalPathName parentTocPath;
    std::map<LocalPathName, off_t> tocOffsets;

    while (
        readNext(*r, walkSubTocs, hideSubTocEntries, hideClearEntries, readMasked, nullptr, nullptr, &parentTocPath)) {

        eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);
        LocalPathName path;
        std::string type;
        bool isSubToc;

        off_t offset;
        std::vector<Index>::iterator j;

        r->dump(out, simple);

        switch (r->header_.tag_) {

            case TocRecord::TOC_INIT: {
                isSubToc = false;
                fdb5::Key key(s);
                if (r->header_.serialisationVersion_ > 1) {
                    s >> isSubToc;
                }
                out << "  key: " << key << ", sub-toc: " << (isSubToc ? "yes" : "no");
                if (!simple) {
                    out << std::endl;
                }
                break;
            }

            case TocRecord::TOC_INDEX: {
                s >> path;
                s >> offset;
                s >> type;
                out << "  path: " << path << ", offset: " << offset << ", type: " << type;
                if (!simple) {
                    out << std::endl;
                }
                Index index(new TocIndex(s, r->header_.serialisationVersion_, currentDirectory(),
                                         currentDirectory() / path, offset));
                index.dump(out, "  ", simple);
                break;
            }

            case TocRecord::TOC_CLEAR: {
                s >> path;
                s >> offset;
                out << "  path: " << path << ", offset: " << offset;
                break;
            }

            case TocRecord::TOC_SUB_TOC: {
                s >> path;
                out << "  path: " << path;
                break;
            }

            default: {
                out << "   Unknown TOC entry";
                break;
            }
        }

        if (dumpStructure) {
            LocalPathName currentToc = currentTocPath();
            if (r->header_.tag_ == TocRecord::TOC_SUB_TOC && subTocRead_ != nullptr) {
                /**
                    currentTocPath will already point to 'child' toc - even though context is still `TOC_SUB_TOC` which
                exists on the 'parent'. to ensure we have consistent offsets, we need to increment offsets for the
                parent toc instead
                **/
                currentToc = parentTocPath;
            }

            out << ", toc-offset: " << tocOffsets[currentToc] << ", length: " << r->header_.size_
                << ", toc-path: " << currentToc;

            tocOffsets[currentToc] += r->header_.size_;
        }
        out << std::endl;
    }
}


void TocHandler::dumpIndexFile(std::ostream& out, const eckit::PathName& indexFile) const {

    openForRead();
    TocHandlerCloser close(*this);

    auto r = std::make_unique<TocRecord>(
        serialisationVersion_.used());  // allocate (large) TocRecord on heap not stack (MARS-779)

    bool walkSubTocs       = true;
    bool hideSubTocEntries = true;
    bool hideClearEntries  = true;
    bool readMasked        = true;
    while (readNext(*r, walkSubTocs, hideSubTocEntries, hideClearEntries, readMasked)) {

        eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);
        LocalPathName path;
        std::string type;
        off_t offset;

        switch (r->header_.tag_) {

            case TocRecord::TOC_INDEX: {
                s >> path;
                s >> offset;
                s >> type;

                if ((currentDirectory() / path).sameAs(eckit::LocalPathName{indexFile})) {
                    r->dump(out, true);
                    out << std::endl << "  Path: " << path << ", offset: " << offset << ", type: " << type;
                    Index index(new TocIndex(s, r->header_.serialisationVersion_, currentDirectory(),
                                             currentDirectory() / path, offset));
                    index.dump(out, "  ", false, true);
                }
                break;
            }

            case TocRecord::TOC_SUB_TOC:
            case TocRecord::TOC_CLEAR:
                ASSERT_MSG(r->header_.tag_ != TocRecord::TOC_CLEAR,
                           "The TOC_CLEAR records should have been pre-filtered on the first pass");
                break;

            case TocRecord::TOC_INIT:
                break;

            default: {
                out << "   Unknown TOC entry" << std::endl;
                break;
            }
        }
    }
}


std::string TocHandler::dbOwner() const {
    return userName(dbUID());
}

DbStats TocHandler::stats() const {
    TocDbStats* stats = new TocDbStats();

    stats->dbCount_ += 1;
    stats->tocRecordsCount_ += numberOfRecords();
    stats->tocFileSize_ += tocFilesSize();
    stats->schemaFileSize_ += schemaPath().size();

    return DbStats(stats);
}


void TocHandler::enumerateMasked(std::set<std::pair<eckit::URI, Offset>>& metadata, std::set<eckit::URI>& data) const {

    if (!enumeratedMaskedEntries_) {
        populateMaskedEntriesList();
    }

    for (const auto& entry : maskedEntries_) {

        ASSERT(entry.first.path().size() > 0);
        eckit::LocalPathName absPath;
        if (entry.first.path()[0] == '/') {
            absPath = entry.first;
            if (!absPath.exists()) {
                absPath = currentDirectory() / entry.first.baseName();
            }
        }
        else {
            absPath = currentDirectory() / entry.first;
        }

        if (absPath.exists()) {
            eckit::URI uri("toc", absPath);
            metadata.insert(std::make_pair(uri, entry.second));

            // If this is a subtoc, then enumerate its contained indexes and data!
            if (uri.path().baseName().asString().substr(0, 4) == "toc.") {
                TocHandler h(absPath, remapKey_);

                h.enumerateMasked(metadata, data);

                std::vector<Index> indexes = h.loadIndexes();
                for (const auto& i : indexes) {
                    metadata.insert(std::make_pair<eckit::URI, Offset>(i.location().uri(), 0));
                    for (const auto& dataURI : i.dataURIs()) {
                        data.insert(dataURI);
                    }
                }
            }
        }
    }

    // Get the data files referenced by the masked indexes (those in subtocs are
    // referenced internally)

    openForRead();
    TocHandlerCloser close(*this);

    auto r = std::make_unique<TocRecord>(
        serialisationVersion_.used());  // allocate (large) TocRecord on heap not stack (MARS-779)

    while (readNextInternal(*r)) {
        if (r->header_.tag_ == TocRecord::TOC_INDEX) {

            eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);

            LocalPathName path;
            std::string type;
            off_t offset;
            s >> path;
            s >> offset;
            s >> type;

            // n.b. readNextInternal --> directory_ not currentDirectory()
            LocalPathName absPath = directory_ / path;

            std::pair<eckit::LocalPathName, size_t> key(absPath.baseName(), offset);
            if (maskedEntries_.find(key) != maskedEntries_.end()) {
                if (absPath.exists()) {
                    Index index(new TocIndex(s, r->header_.serialisationVersion_, directory_, absPath, offset));
                    for (const auto& dataURI : index.dataURIs())
                        data.insert(dataURI);
                }
            }
        }
    }
}


size_t TocHandler::tocFilesSize() const {

    // Get the size of the master toc

    size_t size = tocPath().size();

    // If we have subtocs, we need to get those too!

    std::vector<eckit::PathName> subtocs = subTocPaths();

    for (std::vector<eckit::PathName>::const_iterator i = subtocs.begin(); i != subtocs.end(); ++i) {
        size += i->size();
    }

    return size;
}

std::string TocHandler::userName(long id) const {
    struct passwd* p = getpwuid(id);

    if (p) {
        return p->pw_name;
    }
    else {
        return eckit::Translator<long, std::string>()(id);
    }
}

const Key& TocHandler::currentRemapKey() const {
    if (subTocRead_) {
        return subTocRead_->currentRemapKey();
    }
    else {
        return remapKey_;
    }
}

const LocalPathName& TocHandler::currentDirectory() const {
    if (subTocRead_) {
        return subTocRead_->currentDirectory();
    }
    else {
        return directory_;
    }
}

const LocalPathName& TocHandler::currentTocPath() const {
    if (subTocRead_) {
        return subTocRead_->currentTocPath();
    }
    else {
        return tocPath_;
    }
}

size_t TocHandler::buildIndexRecord(TocRecord& r, const Index& index) {

    const IndexLocation& location(index.location());
    const TocIndexLocation& tocLoc(reinterpret_cast<const TocIndexLocation&>(location));

    ASSERT(r.header_.tag_ == TocRecord::TOC_INDEX);

    eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);

    s << tocLoc.uri().path().baseName();
    s << tocLoc.offset();
    s << index.type();
    index.encode(s, r.header_.serialisationVersion_);

    return s.position();
}

size_t TocHandler::buildClearRecord(TocRecord& r, const Index& index) {

    struct TocIndexLocationExtracter : public IndexLocationVisitor {

        TocIndexLocationExtracter(TocRecord& r) : r_(r), sz_(0) {}

        virtual void operator()(const IndexLocation& l) {

            const TocIndexLocation& location = reinterpret_cast<const TocIndexLocation&>(l);

            eckit::MemoryStream s(&r_.payload_[0], r_.maxPayloadSize);

            s << location.uri().path().baseName();
            s << location.offset();
            ASSERT(sz_ == 0);
            sz_ = s.position();
            LOG_DEBUG_LIB(LibFdb5) << "Write TOC_CLEAR " << location.uri().path().baseName() << " - "
                                   << location.offset() << std::endl;
        }

        size_t size() const { return sz_; }

    private:

        TocRecord& r_;
        size_t sz_;
    };

    ASSERT(r.header_.tag_ == TocRecord::TOC_CLEAR);
    TocIndexLocationExtracter visitor(r);
    index.visit(visitor);
    return visitor.size();
}

size_t TocHandler::buildSubTocMaskRecord(TocRecord& r) {

    /// n.b. We construct a subtoc masking record using TOC_CLEAR for backward compatibility.

    ASSERT(useSubToc_);
    ASSERT(subTocWrite_);

    // We use a relative path to this subtoc if it belongs to the current DB
    // but an absolute one otherwise (e.g. for fdb-overlay).
    const PathName& absPath = subTocWrite_->tocPath();
    PathName path           = (absPath.dirName().sameAs(directory_)) ? absPath.baseName() : absPath;

    return buildSubTocMaskRecord(r, path);
}

size_t TocHandler::buildSubTocMaskRecord(TocRecord& r, const eckit::PathName& path) {

    /// n.b. We construct a subtoc masking record using TOC_CLEAR for backward compatibility.

    ASSERT(r.header_.tag_ == TocRecord::TOC_CLEAR);

    eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);

    s << path;
    s << static_cast<off_t>(0);  // Always use an offset of zero for subtocs

    return s.position();
}

void TocHandler::control(const ControlAction& action, const ControlIdentifiers& identifiers) const {


    for (ControlIdentifier identifier : identifiers) {

        auto it = controlfile_lookup.find(identifier);
        ASSERT(it != controlfile_lookup.end());

        const std::string& lock_file(it->second);

        switch (action) {
            case ControlAction::Disable:
                createControlFile(lock_file);
                break;

            case ControlAction::Enable:
                removeControlFile(lock_file);
                break;

            default:
                eckit::Log::warning() << "Unexpected action: " << static_cast<uint16_t>(action) << std::endl;
        }
    }
}

bool TocHandler::enabled(const ControlIdentifier& controlIdentifier) const {
    auto it = controlfile_lookup.find(controlIdentifier);
    ASSERT(it != controlfile_lookup.end());

    const std::string& control_file(it->second);
    return !fullControlFilePath(control_file).exists();
};

std::vector<PathName> TocHandler::lockfilePaths() const {

    std::vector<PathName> paths;

    for (const auto& name : {retrieve_lock_file, archive_lock_file, list_lock_file, wipe_lock_file}) {

        PathName fullPath = fullControlFilePath(name);
        if (fullPath.exists())
            paths.emplace_back(std::move(fullPath));
    }

    return paths;
}

LocalPathName TocHandler::fullControlFilePath(const std::string& name) const {
    return directory_ / name;
}

void TocHandler::createControlFile(const std::string& name) const {

    checkUID();

    // It is not an error to lock something that is already locked
    PathName fullPath(fullControlFilePath(name));
    if (!fullPath.exists()) {
        fullPath.touch();
    }
}

void TocHandler::removeControlFile(const std::string& name) const {

    checkUID();

    // It is not an error to unlock something that is already unlocked
    PathName fullPath(fullControlFilePath(name));
    if (fullPath.exists()) {
        bool verbose = false;
        fullPath.unlink(verbose);
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

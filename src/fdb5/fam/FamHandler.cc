/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the Horizon Europe programme funded project OpenCUBE
 * (Grant agreement: 101092984) horizon-opencube.eu
 */

#include "fdb5/fam/FamHandler.h"

#include "eckit/config/Resource.h"
#include "eckit/io/fam/FamRegionName.h"
// #include "eckit/filesystem/PathName.h"
// #include "eckit/io/FileDescHandle.h"
// #include "eckit/io/FileHandle.h"
#include "eckit/log/BigNum.h"
#include "eckit/log/Log.h"
#include "eckit/maths/Functions.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/StaticMutex.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/database/Index.h"
#include "fdb5/fam/FamCommon.h"
#include "fdb5/fam/FamFieldLocation.h"
#include "fdb5/fam/FamIndex.h"
// #include "fdb5/fam/TOCStats.h"
// #include "fdb5/io/LustreSettings.h"

#include <fcntl.h>
#include <pwd.h>
#include <sys/types.h>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {
constexpr const char* retrieve_lock_file    = "retrieve.lock";
constexpr const char* archive_lock_file     = "archive.lock";
constexpr const char* list_lock_file        = "list.lock";
constexpr const char* wipe_lock_file        = "wipe.lock";
constexpr const char* allow_duplicates_file = "duplicates.allow";
}  // namespace

const std::map<ControlIdentifier, const char*> controlfile_lookup {
    {ControlIdentifier::Retrieve, retrieve_lock_file},
    {ControlIdentifier::Archive, archive_lock_file},
    {ControlIdentifier::List, list_lock_file},
    {ControlIdentifier::Wipe, wipe_lock_file},
    {ControlIdentifier::UniqueRoot, allow_duplicates_file}};

//----------------------------------------------------------------------------------------------------------------------

class FamHandler::AutoCloser: eckit::NonCopyable {
    const FamHandler& handler_;

public:
    explicit AutoCloser(const FamHandler& handler): handler_(handler) { }

    ~AutoCloser() { handler_.close(); }
};

//----------------------------------------------------------------------------------------------------------------------

class CachedFDProxy {
public:  // methods
    CachedFDProxy(const eckit::FamObjectName& path, int fd, std::unique_ptr<eckit::MemoryHandle>& cached):
        path_(path), fd_(fd), cached_(cached.get()) {
        ASSERT((fd != -1) != (!!cached));
    }

    long read(void* buf, long len) {
        if (cached_) {
            return cached_->read(buf, len);
        } else {
            long ret = 0;
            // SYSCALL2(ret = ::read(fd_, buf, len), path_);
            return ret;
        }
    }

    eckit::Offset position() {
        if (cached_) {
            return cached_->position();
        } else {
            off_t pos = 0;
            // SYSCALL(pos = ::lseek(fd_, 0, SEEK_CUR));
            return pos;
        }
    }

    eckit::Offset seek(const eckit::Offset& pos) {
        if (cached_) {
            return cached_->seek(pos);
        } else {
            off_t ret = 0;
            // SYSCALL(ret = ::lseek(fd_, pos, SEEK_SET));
            // ASSERT(ret == pos);
            return pos;
        }
    }

private:  // members
    const eckit::FamObjectName& path_;
    int                         fd_;
    eckit::MemoryHandle*        cached_;
};

//----------------------------------------------------------------------------------------------------------------------

FamHandler::FamHandler(const eckit::FamRegionName& region, const Config& config):
    // config_ {config},
    handler_ {region.object("toc")},
    version_ {FamRecordVersion(config)},
    useSubToc_ {config.userConfig().getBool("useSubToc", false)} { }

FamHandler::FamHandler(const eckit::FamRegionName& region, const Key& parentKey):
    parentKey_ {parentKey}, handler_ {region.object("toc")}, version_(FamRecordVersion(Config {})), isSubToc_ {true} {
    /// Are we remapping a mounted DB?
    if (exists()) {
        const Key key(databaseKey());
        if (!parentKey.empty() && parentKey != key) {
            LOG_DEBUG_LIB(LibFdb5) << "Opening (remapped) toc with differing parent key: " << key << " --> "
                                   << parentKey << std::endl;

            if (parentKey.size() != key.size()) {
                std::stringstream ss;
                ss << "Keys insufficiently matching for mount: " << key << " : " << parentKey;
                throw eckit::UserError(ss.str(), Here());
            }

            for (const auto& kv : parentKey) {
                auto it = key.find(kv.first);
                if (it == key.end()) {
                    std::stringstream ss;
                    ss << "Keys insufficiently matching for mount: " << key << " : " << parentKey;
                    throw eckit::UserError(ss.str(), Here());
                }
                if (kv.second != it->second) { remapKey_.set(kv.first, kv.second); }
            }

            LOG_DEBUG_LIB(LibFdb5) << "Key remapping: " << remapKey_ << std::endl;
        }
    }
}

FamHandler::~FamHandler() {
    LOG_DEBUG_LIB(LibFdb5) << __func__ << " " << handler_ << '\n';
    close();
}

bool FamHandler::exists() const {
    LOG_DEBUG_LIB(LibFdb5) << __func__ << " " << handler_ << '\n';
    return handler_.exists();
}

void FamHandler::open(const Mode mode) const {
    LOG_DEBUG_LIB(LibFdb5) << __func__ << " " << handler_ << '\n';
    // ASSERT(!toc_);
    ASSERT(mode_ == Mode::CLOSED);
    mode_ = mode;
}

void FamHandler::openForAppend() {
    open(Mode::WRITE);

    if (cached_) { cached_.reset(); }

    // ASSERT(toc_);

    LOG_DEBUG_LIB(LibFdb5) << __func__ << " " << handler_ << '\n';

    //     int iomode = O_WRONLY | O_APPEND;
    // #ifdef O_NOATIME
    //     // this introduces issues of permissions
    //     static bool fdbNoATime = eckit::Resource<bool>("fdbNoATime;$FDB_OPEN_NOATIME", false);
    //     if (fdbNoATime) { iomode |= O_NOATIME; }
    // #endif
    //     SYSCALL2((fd_ = ::open(handler_.localPath(), iomode, (mode_t)0777)), handler_);
}

void FamHandler::openForRead() const {
    open(Mode::READ);

    if (cached_) {
        cached_->seek(0);
        return;
    }

    // static bool fdbCacheTOCSOnRead = eckit::Resource<bool>("fdbCacheTOCSOnRead;$FDB_CACHE_TOCS_ON_READ", true);

    // ASSERT(fd_ == -1);

    // writeMode_ = false;

    LOG_DEBUG_LIB(LibFdb5) << __func__ << " " << handler_ << '\n';

    //     int iomode = O_RDONLY;
    // #ifdef O_NOATIME
    //     // this introduces issues of permissions
    //     static bool fdbNoATime = eckit::Resource<bool>("fdbNoATime;$FDB_OPEN_NOATIME", false);
    //     if (fdbNoATime) { iomode |= O_NOATIME; }
    // #endif
    //     SYSCALL2((fd_ = ::open(handler_.localPath(), iomode)), handler_);
    //
    //     eckit::Length TOCSize = handler_.size();
    //
    //     // The masked subTOCS and indexes could be updated each time, so reset this.
    //     enumerated_ = false;
    //     masked_.clear();
    //
    //     if (fdbCacheTOCSOnRead) {
    //         FileDescHandle Fam(fd_, true);  // closes the file descriptor
    //         AuFamlose      closer1(Fam);
    //         fd_ = -1;
    //
    //         bool grow = true;
    //         cached_.reset(new eckit::MemoryHandle(TOCSize, grow));
    //
    //         long buffersize = 4 * 1024 * 1024;
    //         Fam.copyTo(*cached_, buffersize, TOCSize, FamReadStats_);
    //         cached_->openForRead();
    //     }
}

void FamHandler::dumpCache() const {
    if (cached_) {
        const eckit::Offset offset = cached_->position();
        cached_->seek(0);

        const eckit::PathName dumpFile("dump_of_" + handler_.asString());
        eckit::FileHandle     dump(eckit::PathName::unique(dumpFile));
        cached_->copyTo(dump);

        std::ostringstream ss;
        ss << handler_.asString()
           << " read in "
           /* << FamReadStats_.size() << " step" << ((FamReadStats_.size() > 1) ? "s" : "") */
           << std::endl;
        // double        time;
        // eckit::Length len;
        // while (FamReadStats_.next(time, len)) {
        //     ss << "  step duration: " << (time * 1000) << " ms, size: " << len << " bytes" << std::endl;
        // }
        eckit::Log::error() << ss.str();

        cached_->seek(offset);
    }
}

void FamHandler::append(FamRecord& rec, size_t payloadSize) {
    // ASSERT(fd_ != -1);
    ASSERT(!cached_);

    LOG_DEBUG_LIB(LibFdb5) << "Writing Fam entry: " << (int)rec.header_.tag_ << std::endl;

    // Obtain the rounded size, and set it in the record header.
    auto roundedSize = roundRecord(rec, payloadSize);

    // size_t len;
    // SYSCALL2(len = ::write(fd_, &rec, roundedSize), handler_);
    // dirty_ = true;
    // ASSERT(len == roundedSize);
}

void FamHandler::appendBlock(const void* data, size_t size) {
    openForAppend();
    const AutoCloser close(*this);

    // ASSERT(fd_ != -1);
    ASSERT(!cached_);

    // Ensure that this block is appropriately rounded.

    ASSERT(size % recordRoundSize() == 0);

    // size_t len;
    // SYSCALL2(len = ::write(fd_, data, size), handler_);
    // dirty_ = true;
    // ASSERT(len == size);
}

size_t FamHandler::recordRoundSize() {
    static const auto size = eckit::Resource<size_t>("fdbRoundTocRecords", 1024);
    return size;
}

size_t FamHandler::roundRecord(FamRecord& rec, size_t payloadSize) {
    rec.header_.size_ = eckit::round(sizeof(FamRecord::Header) + payloadSize, recordRoundSize());
    return rec.header_.size_;
}

// readNext wraps readNextInternal.
// readNext reads the next Fam entry from this Fam, or from an appropriate subtoc if necessary.
bool FamHandler::readNext(FamRecord& rec,
                          bool       walkSubTOCS,
                          bool       hideSubTocEntries,
                          bool       hideClearEntries,
                          bool       readMasked) const {
    int len;

    // Ensure we are able to skip masked entries as appropriate

    if (!enumerated_) { populateMaskedEntriesList(); }

    // For some tools (mainly diagnostic) it makes sense to be able to switch the
    // walking behaviour here.

    if (!walkSubTOCS) { return readNextInternal(rec); }

    // while (true) {
    // if (subTocRead_) {
    // len = subTocRead_->readNext(rec, walkSubTOCS, hideSubTocEntries, hideClearEntries, readMasked);
    // if (len == 0) {
    // subTocRead_.reset();
    // } else {
    // ASSERT(rec.header_.tag_ != FamRecord::Tag::TOC_SUBTOC);
    // return true;
    // }
    // } else {
    // if (!readNextInternal(rec)) {
    // return false;
    //
    // } else if (rec.header_.tag_ == FamRecord::Tag::TOC_INIT) {
    // eckit::MemoryStream s(&rec.payload_[0], rec.capacity);
    // if (parentKey_.empty()) { parentKey_ = Key(s); }
    // return true;
    //
    // } else if (rec.header_.tag_ == FamRecord::Tag::TOC_SUBTOC) {
    // eckit::MemoryStream s(&rec.payload_[0], rec.capacity);
    // eckit::PathName     path;
    // s >> path;
    // // Handle both path and absPath for compatibility as we move from storing
    // // absolute paths to relative paths. Either may exist in either the Fam_SUBTOC
    // // or Fam_CLEAR entries.
    // ASSERT(path.path().size() > 0);
    // eckit::PathName absPath;
    // if (path.path()[0] == '/') {
    // absPath = findRealPath(path);
    // if (!absPath.exists()) { absPath = currentDirectory() / path.baseName(); }
    // } else {
    // absPath = currentDirectory() / path;
    // }
    //
    // // If this subtoc has a masking entry, then skip it, and go on to the next entry.
    // // Unless readMasked is true, in which case walk it if it exists.
    // std::pair<eckit::PathName, size_t> key(absPath.baseName(), 0);
    // if (masked_.find(key) != masked_.end()) {
    // if (!readMasked) {
    // LOG_DEBUG_LIB(LibFdb5) << "SubToc ignored by mask: " << path << std::endl;
    // continue;
    // }
    // // This is a masked subtoc, so it is valid for it to not exist.
    // if (!absPath.exists()) {
    // LOG_DEBUG_LIB(LibFdb5) << "SubToc does not exist: " << path << std::endl;
    // continue;
    // }
    // }
    //
    // LOG_DEBUG_LIB(LibFdb5) << "Opening SUB_Fam: " << absPath << " " << parentKey_ << std::endl;
    //
    // subTocRead_.reset(new FamHandler(absPath, parentKey_));
    // subTocRead_->openForRead();
    //
    // if (hideSubTocEntries) {
    // // The first entry in a subtoc must be the init record. Check that
    // subTocRead_->readNext(rec, walkSubTOCS, hideSubTocEntries, hideClearEntries, readMasked);
    // ASSERT(rec.header_.tag_ == FamRecord::Tag::TOC_INIT);
    // } else {
    // return true;  // if not hiding the subtoc entries, return them as normal entries!
    // }
    //
    // } else if (rec.header_.tag_ == FamRecord::Tag::TOC_INDEX) {
    // eckit::MemoryStream s(&rec.payload_[0], rec.capacity);
    // eckit::PathName     path;
    // off_t               offset;
    // s >> path;
    // s >> offset;
    //
    // eckit::PathName absPath = currentDirectory() / path;
    //
    // std::pair<eckit::PathName, size_t> key(absPath.baseName(), offset);
    // if (masked_.find(key) != masked_.end()) {
    // if (!readMasked) {
    // LOG_DEBUG_LIB(LibFdb5) << "Index ignored by mask: " << path << ":" << offset << std::endl;
    // continue;
    // }
    // // This is a masked index, so it is valid for it to not exist.
    // if (!absPath.exists()) {
    // LOG_DEBUG_LIB(LibFdb5) << "Index does not exist: " << path << ":" << offset << std::endl;
    // continue;
    // }
    // }
    //
    // return true;
    //
    // } else if (rec.header_.tag_ == FamRecord::Tag::TOC_CLEAR && hideClearEntries) {
    // continue;  // we already handled the Fam_CLEAR entries in populateMaskedEntriesList()
    // } else {
    // // A normal read operation
    // return true;
    // }
    // }
    // }
}

// readNext wraps readNextInternal.
// readNextInternal reads the next Fam entry from this Fam.
bool FamHandler::readNextInternal(FamRecord& rec) const {
    // CachedFDProxy proxy(handler_, fd_, cached_);
    CachedFDProxy proxy(handler_, 1, cached_);

    try {
        long len = proxy.read(&rec, sizeof(FamRecord::Header));
        if (len == 0) { return false; }
        ASSERT(len == sizeof(FamRecord::Header));
    } catch (...) {
        dumpCache();
        throw;
    }

    try {
        long len = proxy.read(&rec.payload_, rec.header_.size_ - sizeof(FamRecord::Header));
        ASSERT(size_t(len) == rec.header_.size_ - sizeof(FamRecord::Header));
    } catch (...) {
        dumpCache();
        throw;
    }

    version_.check(rec.header_.version_, true);

    return true;
}

std::vector<eckit::PathName> FamHandler::subTocPaths() const {
    openForRead();
    const AutoCloser close(*this);

    std::unique_ptr<FamRecord> rec(new FamRecord(version_.value()));  // allocate (large) FamRecord on heap
                                                                      // not stack (MARS-779)

    std::vector<eckit::PathName> paths;

    // bool walkSubTOCS       = true;
    // bool hideSubTocEntries = false;
    // bool hideClearEntries  = true;
    // while (readNext(*rec, walkSubTOCS, hideSubTocEntries, hideClearEntries)) {
    // eckit::MemoryStream s(&rec->payload_[0], rec->capacity);
    // std::string         path;
    // switch (rec->header_.tag_) {
    // case FamRecord::Tag::TOC_SUBTOC: {
    // // n.b. don't just push path onto paths, as it may be relative to a subtoc
    // //      in a different DB (e.g. through an overlay). So query it properly.
    // ASSERT(subTocRead_);
    // paths.push_back(currentToc());
    // break;
    // }
    // case FamRecord::Tag::TOC_CLEAR:
    // ASSERT_MSG(rec->header_.tag_ != FamRecord::Tag::TOC_CLEAR,
    //         "The Fam_CLEAR records should have been pre-filtered on the first pass");
    // break;
    // case FamRecord::Tag::TOC_INIT:  break;
    // case FamRecord::Tag::TOC_INDEX: break;
    // default:                        {
    // // This is only a warning, as it is legal for later versions of software to add stuff
    // // that is just meaningless in a backwards-compatible sense.
    // eckit::Log::warning() << "Unknown Fam entry " << (*rec) << " @ " << Here() << std::endl;
    // break;
    // }
    // }
    // }

    return paths;
}

void FamHandler::close() const {
    LOG_DEBUG_LIB(LibFdb5) << __func__ << " " << handler_ << '\n';

    if (subTocRead_) {
        subTocRead_->close();
        subTocRead_.reset();
    }

    if (subTocWrite_) {
        // We keep track of the subtoc we are writing to until the process is closed, so don't reset
        // the pointer here (or we will create a proliferation of sub TOCS)
        subTocWrite_->close();
    }

    // if (fd_ >= 0) {
    //     LOG_DEBUG_LIB(LibFdb5) << "Closing Fam " << handler_ << std::endl;
    //     if (dirty_) {
    //         SYSCALL2(eckit::fdatasync(fd_), handler_);
    //         dirty_ = false;
    //     }
    //     SYSCALL2(::close(fd_), handler_);
    //     fd_        = -1;
    //     writeMode_ = false;
    // }
}

void FamHandler::allMaskableEntries(eckit::Offset                                        startOffset,
                                    eckit::Offset                                        endOffset,
                                    std::set<std::pair<eckit::PathName, eckit::Offset>>& maskedEntries) const {
    CachedFDProxy proxy(handler_, 1, cached_);

    // Start reading entries where we are told to

    eckit::Offset ret = proxy.seek(startOffset);
    ASSERT(ret == startOffset);

    std::unique_ptr<FamRecord> rec(new FamRecord(version_.value()));  // allocate (large) FamRecord on heap
                                                                      // not stack (MARS-779)

    while (proxy.position() < endOffset) {
        ASSERT(readNextInternal(*rec));

        eckit::MemoryStream s(&rec->payload_[0], rec->capacity);
        std::string         path;
        off_t               offset;

        switch (rec->header_.tag_) {
            case FamRecord::Tag::TOC_SUBTOC: {
                s >> path;
                eckit::PathName eckit::pathName = path;
                // ASSERT(path.size() > 0);
                // eckit::PathName absPath = (path[0] == '/') ? findRealPath(path) : (currentDirectory() / path);
                maskedEntries.emplace(std::pair<eckit::PathName, eckit::Offset>(pathName.baseName(), 0));
                break;
            }
            case FamRecord::Tag::TOC_INDEX:
                s >> path;
                s >> offset;
                // readNextInternal --> use directory_ not currentDirectory()
                maskedEntries.emplace(std::pair<eckit::PathName, eckit::Offset>(path, offset));
                break;
            case FamRecord::Tag::TOC_CLEAR: break;
            case FamRecord::Tag::TOC_INIT:  break;
            default:                        {
                // This is only a warning, as it is legal for later versions of software to add stuff
                // that is just meaningless in a backwards-compatible sense.
                eckit::Log::warning() << "Unknown Fam entry " << (*rec) << " @ " << Here() << std::endl;
                break;
            }
        }
    }
}

void FamHandler::populateMaskedEntriesList() const {
    // ASSERT(fd_ != -1 || cached_);
    CachedFDProxy proxy(handler_, 1, cached_);

    eckit::Offset startPosition = proxy.position();  // remember the current position of the file descriptor

    masked_.clear();

    std::unique_ptr<FamRecord> rec(new FamRecord(version_.value()));  // allocate (large) FamRecord on heap
                                                                      // not stack (MARS-779)

    while (readNextInternal(*rec)) {
        eckit::MemoryStream s(&rec->payload_[0], rec->capacity);
        std::string         path;
        off_t               offset;

        switch (rec->header_.tag_) {
            case FamRecord::Tag::TOC_CLEAR: {
                s >> path;
                s >> offset;
                if (path == "*") {  // For the "*" path, mask EVERYTHING that we have already seen
                    eckit::Offset currentPosition = proxy.position();
                    allMaskableEntries(startPosition, currentPosition, masked_);
                    ASSERT(currentPosition == proxy.position());
                } else {
                    // readNextInternal --> use directory_ not currentDirectory()
                    // ASSERT(path.size() > 0);
                    // eckit::PathName absPath = (path[0] == '/') ? findRealPath(path) : (directory_ / path);
                    eckit::PathName eckit::pathName = path;
                    masked_.emplace(std::pair<eckit::PathName, eckit::Offset>(pathName.baseName(), offset));
                }
                break;
            }
            case FamRecord::Tag::TOC_SUBTOC: break;
            case FamRecord::Tag::TOC_INIT:   break;
            case FamRecord::Tag::TOC_INDEX:  break;
            default:                         {
                // This is only a warning, as it is legal for later versions of software to add stuff
                // that is just meaningless in a backwards-compatible sense.
                eckit::Log::warning() << "Unknown Fam entry " << (*rec) << " @ " << Here() << std::endl;
                break;
            }
        }
    }

    // And reset back to where we were.

    eckit::Offset ret = proxy.seek(startPosition);
    ASSERT(ret == startPosition);

    enumerated_ = true;
}

static eckit::StaticMutex local_mutex;

void FamHandler::writeInitRecord(const Key& key) {
    const eckit::AutoLock<eckit::StaticMutex> lock(local_mutex);

    // if (!directory_.exists()) { directory_.mkdir(); }

    // // enforce lustre striping if requested
    // if (stripeLustre() && !handler_.exists()) {
    //     fdb5LustreapiFileCreate(handler_.localPath(), stripeIndexLustreSettings());
    // }

    // ASSERT(fd_ == -1);
    // int iomode = O_CREAT | O_RDWR;
    // SYSCALL2(fd_ = ::open(handler_.localPath(), iomode, mode_t(0777)), handler_);

    const AutoCloser closer(*this);

    std::unique_ptr<FamRecord> rec(new FamRecord(version_.value()));

    size_t len = readNext(*rec);
    if (len == 0) {
        LOG_DEBUG_LIB(LibFdb5) << "Initializing FDB Fam in " << handler_ << std::endl;

        if (!isSubToc_) {
            /* Copy schema first */

            LOG_DEBUG_LIB(LibFdb5) << "Copy schema from " << config_.schemaPath() << " to " << schemaPath_ << std::endl;

            eckit::PathName tmp = eckit::PathName::unique(schemaPath_);

            eckit::FileHandle in(config_.schemaPath());

            // Enforce lustre striping if requested

            // SDS: Would be nicer to do this, but FileHandle doesn't have a path_ member, let alone an exposed one
            //      so would need some tinkering to work with LustreFileHandle.
            // LustreFileHandle<eckit::FileHandle> out(tmp, stripeIndexLustreSettings());

            // if (stripeLustre()) { fdb5LustreapiFileCreate(tmp.localPath(), stripeIndexLustreSettings()); }
            eckit::FileHandle out(tmp);
            in.copyTo(out);

            eckit::PathName::rename(tmp, schemaPath_);
        }

        std::unique_ptr<FamRecord> r2(new FamRecord(version_.value(), FamRecord::Tag::TOC_INIT));

        eckit::MemoryStream s(&r2->payload_[0], r2->capacity);

        s << key;
        s << isSubToc_;
        append(*r2, s.position());
        // dbUID_ = r2->header_.uid_;

    } else {
        ASSERT(rec->header_.tag_ == FamRecord::Tag::TOC_INIT);
        eckit::MemoryStream s(&rec->payload_[0], rec->capacity);
        ASSERT(key == Key(s));
        // dbUID_ = rec->header_.uid_;
    }
}

void FamHandler::writeClearRecord(const Index& index) {
    std::unique_ptr<FamRecord> rec(new FamRecord(version_.value(), FamRecord::Tag::TOC_CLEAR));

    size_t sz = roundRecord(*rec, buildClearRecord(*rec, index));
    appendBlock(rec.get(), sz);
}

void FamHandler::writeClearAllRecord() {
    std::unique_ptr<FamRecord> rec(new FamRecord(version_.value(), FamRecord::Tag::TOC_CLEAR));

    eckit::MemoryStream s(&rec->payload_[0], rec->capacity);
    s << std::string {"*"};
    s << off_t {0};

    size_t sz = roundRecord(*rec, s.position());
    appendBlock(rec.get(), sz);
}

void FamHandler::writeSubTocRecord(const FamHandler& subtoc) {
    openForAppend();
    const AutoCloser closer(*this);

    std::unique_ptr<FamRecord> rec(new FamRecord(version_.value(), FamRecord::Tag::TOC_SUBTOC));

    eckit::MemoryStream s(&rec->payload_[0], rec->capacity);

    // We use a relative path to this subtoc if it belongs to the current DB
    // but an absolute one otherwise (e.g. for fdb-overlay).
    const eckit::PathName& absPath = subtoc.famPath();
    eckit::PathName        path    = (absPath.dirName().sameAs(directory_)) ? absPath.baseName() : absPath;

    s << path;
    s << off_t {0};
    append(*rec, s.position());

    LOG_DEBUG_LIB(LibFdb5) << "Write Fam_SUBTOC " << path << std::endl;
}

void FamHandler::writeIndexRecord(const Index& index) {
    // Fam index writer

    struct WriteToStream: public IndexLocationVisitor {
        WriteToStream(const Index& index, FamHandler& handler): index_(index), handler_(handler) { }

        virtual void operator()(const IndexLocation& l) {
            const FamIndexLocation& location = reinterpret_cast<const FamIndexLocation&>(l);

            std::unique_ptr<FamRecord> rec(new FamRecord(handler_.version_.value(), FamRecord::Tag::TOC_INDEX));

            eckit::MemoryStream s(&rec->payload_[0], rec->capacity);

            s << location.uri().path().baseName();
            // s << location.offset();
            s << index_.type();

            index_.encode(s, rec->header_.version_);
            handler_.append(*rec, s.position());

            LOG_DEBUG_LIB(LibFdb5) << "Write Fam_INDEX " << location.uri().path().baseName() << " - "
                                   << /* location.offset() << */ " " << index_.type() << std::endl;
        }

    private:
        const Index& index_;
        FamHandler&  handler_;
    };

    // If we are using a subtoc, delegate there

    if (useSubToc_) {
        // Create the subtoc, and insert the redirection record into the the master Fam.

        if (!subTocWrite_) {
            eckit::PathName subtoc = eckit::PathName::unique("Fam");

            subTocWrite_.reset(new FamHandler(currentDirectory() / subtoc, Key {}));

            subTocWrite_->writeInitRecord(databaseKey());

            writeSubTocRecord(*subTocWrite_);
        }

        subTocWrite_->writeIndexRecord(index);
        return;
    }

    // Otherwise, we actually do the writing!

    openForAppend();
    AutoCloser closer(*this);

    WriteToStream writeVisitor(index, *this);
    index.visit(writeVisitor);
}

void FamHandler::writeSubTocMaskRecord(const FamHandler& subtoc) {
    std::unique_ptr<FamRecord> rec(new FamRecord(version_.value(), FamRecord::Tag::TOC_CLEAR));

    // We use a relative path to this subtoc if it belongs to the current DB
    // but an absolute one otherwise (e.g. for fdb-overlay).
    const eckit::PathName& absPath = subtoc.famPath();
    eckit::PathName        path    = (absPath.dirName().sameAs(directory_)) ? absPath.baseName() : absPath;

    size_t sz = roundRecord(*rec, buildSubTocMaskRecord(*rec, path));
    appendBlock(rec.get(), sz);
}

bool FamHandler::useSubToc() const {
    return useSubToc_;
}

bool FamHandler::anythingWrittenToSubToc() const {
    return !!subTocWrite_;
}

//----------------------------------------------------------------------------------------------------------------------

class HasPath {
    eckit::PathName path_;
    off_t           offset_;

public:
    HasPath(const eckit::PathName& path, off_t offset): path_(path), offset_(offset) { }

    bool operator()(const Index index) const {
        const FamIndex* Famidx = dynamic_cast<const FamIndex*>(index.content());

        if (!Famidx) { throw eckit::NotImplemented("Unknown index type: " + index.type(), Here()); }

        return (Famidx->path() == path_) && (Famidx->offset() == offset_);
    }
};

Key FamHandler::databaseKey() {
    openForRead();
    AutoCloser close(*this);

    // Allocate (large) FamRecord on heap not stack (MARS-779)
    std::unique_ptr<FamRecord> rec(new FamRecord(*version_));

    while (readNext(*rec)) {
        if (rec->header_.tag_ == FamRecord::Tag::TOC_INIT) {
            eckit::MemoryStream s(&rec->payload_[0], rec->capacity);
            dbUID_ = rec->header_.uid_;
            return Key(s);
        }
    }

    throw eckit::SeriousBug("Cannot find a Fam_INIT record");
}

size_t FamHandler::numberOfRecords() const {
    if (count_ == 0) {
        openForRead();
        AutoCloser close(*this);

        // Allocate (large) FamRecord on heap not stack (MARS-779)
        std::unique_ptr<FamRecord> rec(new FamRecord(*version_));

        bool walkSubTOCS       = true;
        bool hideSubTocEntries = false;
        bool hideClearEntries  = false;
        while (readNext(*rec, walkSubTOCS, hideSubTocEntries, hideClearEntries)) { count_++; }
    }

    return count_;
}

// const eckit::PathName& FamHandler::directory() const {
//     return directory_;
// }

std::vector<Index> FamHandler::loadIndexes(bool                   sorted,
                                           std::set<std::string>* subTOCS,
                                           std::vector<bool>*     indexInSubToc,
                                           std::vector<Key>*      remapKeys) const {
    std::vector<Index> indexes;

    if (!handler_.exists()) { return indexes; }

    openForRead();
    AutoCloser close(*this);

    // Allocate (large) FamRecord on heap not stack (MARS-779)
    std::unique_ptr<FamRecord> rec(new FamRecord(version_.value()));
    count_ = 0;

    bool debug             = LibFdb5::instance().debug();
    bool walkSubTOCS       = true;
    bool hideSubTocEntries = true;
    bool hideClearEntries  = true;
    while (readNext(*rec, walkSubTOCS, hideSubTocEntries, hideClearEntries)) {
        eckit::MemoryStream s(&rec->payload_[0], rec->capacity);
        std::string         path;
        std::string         type;

        off_t                        offset;
        std::vector<Index>::iterator j;

        count_++;

        switch (rec->header_.tag_) {
            case FamRecord::Tag::TOC_INIT:
                dbUID_ = rec->header_.uid_;
                LOG_DEBUG(debug, LibFdb5) << "FamRecord Fam_INIT key is " << Key(s) << std::endl;
                break;

            case FamRecord::Tag::TOC_INDEX:
                s >> path;
                s >> offset;
                s >> type;
                LOG_DEBUG(debug, LibFdb5) << "FamRecord Fam_INDEX " << path << " - " << offset << std::endl;
                indexes.push_back(new FamIndex(s,
                                               rec->header_.version_,
                                               currentDirectory(),
                                               currentDirectory() / path,
                                               offset,
                                               preloadBTree_));

                if (subTOCS != 0 && subTocRead_) { subTOCS->insert(subTocRead_->famPath()); }
                if (indexInSubToc) { indexInSubToc->push_back(!!subTocRead_); }
                if (remapKeys) { remapKeys->push_back(currentRemapKey()); }
                break;

            case FamRecord::Tag::TOC_CLEAR:
                ASSERT_MSG(rec->header_.tag_ != FamRecord::Tag::TOC_CLEAR,
                           "The Fam_CLEAR records should have been pre-filtered on the first pass");
                break;

            case FamRecord::Tag::TOC_SUBTOC:
                throw eckit::SeriousBug("Fam_SUBTOC entry should be handled inside readNext");
                break;

            default:
                std::ostringstream oss;
                oss << "Unknown tag in FamRecord " << *rec;
                throw eckit::SeriousBug(oss.str(), Here());
                break;
        }
    }

    // For some purposes, it is useful to have the indexes sorted by their location, as this is is faster for
    // iterating through the data.

    if (sorted) {
        ASSERT(!indexInSubToc);
        ASSERT(!remapKeys);
        std::sort(indexes.begin(), indexes.end(), FamIndexSort());

    } else {
        // In the normal case, the entries are sorted into reverse order. The last index takes precedence
        std::reverse(indexes.begin(), indexes.end());

        if (indexInSubToc) { std::reverse(indexInSubToc->begin(), indexInSubToc->end()); }
        if (remapKeys) { std::reverse(remapKeys->begin(), remapKeys->end()); }
    }

    return indexes;
}

const eckit::PathName& FamHandler::famPath() const {
    return handler_;
}

const eckit::PathName& FamHandler::schemaPath() const {
    return schemaPath_;
}

void FamHandler::dump(std::ostream& out, bool simple, bool walkSubTOCS) const {
    openForRead();
    const AutoCloser close(*this);

    // Allocate (large) FamRecord on heap not stack (MARS-779)
    std::unique_ptr<FamRecord> rec(new FamRecord(*version_));

    bool hideSubTocEntries = false;
    bool hideClearEntries  = false;
    while (readNext(*rec, walkSubTOCS, hideSubTocEntries, hideClearEntries)) {
        eckit::MemoryStream s(&rec->payload_[0], rec->capacity);
        std::string         path;
        std::string         type;
        bool                isSubToc;

        off_t                        offset;
        std::vector<Index>::iterator j;

        rec->dump(out);

        switch (rec->header_.tag_) {
            case FamRecord::Tag::TOC_INIT: {
                isSubToc = false;
                fdb5::Key key(s);
                if (rec->header_.version_ > 1) { s >> isSubToc; }
                out << "  Key: " << key << ", sub-Fam: " << (isSubToc ? "yes" : "no");
                if (!simple) { out << std::endl; }
                break;
            }

            case FamRecord::Tag::TOC_INDEX: {
                s >> path;
                s >> offset;
                s >> type;
                out << "  Path: " << path << ", offset: " << offset << ", type: " << type;
                if (!simple) { out << std::endl; }
                Index index(new FamIndex(s, rec->header_.version_, currentDirectory(), currentDirectory() / path, offset));
                index.dump(out, "  ", simple);
                break;
            }

            case FamRecord::Tag::TOC_CLEAR: {
                s >> path;
                s >> offset;
                out << "  Path: " << path << ", offset: " << offset;
                break;
            }

            case FamRecord::Tag::TOC_SUBTOC: {
                s >> path;
                out << "  Path: " << path;
                break;
            }

            default: {
                out << "   Unknown Fam entry";
                break;
            }
        }
        out << std::endl;
    }
}

void FamHandler::dumpIndex(std::ostream& out, const eckit::FamObjectName& index) const {
    openForRead();
    AutoCloser close(*this);

    // Allocate (large) FamRecord on heap not stack (MARS-779)
    std::unique_ptr<FamRecord> rec(new FamRecord(version_.value()));

    bool walkSubTOCS       = true;
    bool hideSubTocEntries = true;
    bool hideClearEntries  = true;
    bool readMasked        = true;
    while (readNext(*rec, walkSubTOCS, hideSubTocEntries, hideClearEntries, readMasked)) {
        eckit::MemoryStream s(&rec->payload_[0], rec->capacity);
        std::string         path;
        std::string         type;
        off_t               offset;

        switch (rec->header_.tag_) {
            case FamRecord::Tag::TOC_INDEX: {
                s >> path;
                s >> offset;
                s >> type;

                if ((currentDirectory() / path).sameAs(index)) {
                    rec->dump(out, true);
                    out << std::endl << "  Path: " << path << ", offset: " << offset << ", type: " << type;
                    Index index(
                        new FamIndex(s, rec->header_.version_, currentDirectory(), currentDirectory() / path, offset));
                    index.dump(out, "  ", false, true);
                }
                break;
            }

            case FamRecord::Tag::TOC_SUBTOC:
            case FamRecord::Tag::TOC_CLEAR:
                ASSERT_MSG(rec->header_.tag_ != FamRecord::Tag::TOC_CLEAR,
                           "The Fam_CLEAR records should have been pre-filtered on the first pass");
                break;

            case FamRecord::Tag::TOC_INIT: break;

            default:                       {
                out << "   Unknown Fam entry" << std::endl;
                break;
            }
        }
    }
}

// std::string FamHandler::dbOwner() const {
//     return userName(dbUID());
// }

// DbStats FamHandler::stats() const {
//     FamDbStats* stats = new FamDbStats();
//     stats->dbCount_         += 1;
//     stats->FamRecordsCount_ += numberOfRecords();
//     stats->FamFileSize_     += FamFilesSize();
//     stats->schemaFileSize_  += schemaPath().size();
//     return DbStats(stats);
// }

void FamHandler::enumerateMasked(std::set<std::pair<eckit::URI, Offset>>& metadata, std::set<eckit::URI>& data) const {
    if (!enumerated_) { populateMaskedEntriesList(); }

    for (const auto& entry : masked_) {
        ASSERT(entry.first.path().size() > 0);
        eckit::PathName absPath;
        if (entry.first.path()[0] == '/') {
            absPath = entry.first;
            if (!absPath.exists()) { absPath = currentDirectory() / entry.first.baseName(); }
        } else {
            absPath = currentDirectory() / entry.first;
        }

        if (absPath.exists()) {
            eckit::URI uri("Fam", absPath);
            metadata.insert(std::make_pair(uri, entry.second));

            // If this is a subtoc, then enumerate its contained indexes and data!
            if (uri.path().baseName().asString().substr(0, 4) == "Fam.") {
                FamHandler h(absPath, remapKey_);

                h.enumerateMasked(metadata, data);

                std::vector<Index> indexes = h.loadIndexes();
                for (const auto& i : indexes) {
                    metadata.insert(std::make_pair<eckit::URI, Offset>(i.location().uri(), 0));
                    for (const auto& dataURI : i.dataURIs()) { data.insert(dataURI); }
                }
            }
        }
    }

    // Get the data files referenced by the masked indexes (those in subTOCS are
    // referenced internally)

    openForRead();
    AutoCloser close(*this);

    // Allocate (large) FamRecord on heap not stack (MARS-779)
    std::unique_ptr<FamRecord> rec(new FamRecord(version_.value()));

    while (readNextInternal(*rec)) {
        if (rec->header_.tag_ == FamRecord::Tag::TOC_INDEX) {
            eckit::MemoryStream s(&rec->payload_[0], rec->capacity);

            std::string path;
            std::string type;
            off_t       offset;
            s >> path;
            s >> offset;
            s >> type;

            // n.b. readNextInternal --> directory_ not currentDirectory()
            eckit::PathName absPath = directory_ / path;

            std::pair<eckit::PathName, size_t> key(absPath.baseName(), offset);
            if (masked_.find(key) != masked_.end()) {
                if (absPath.exists()) {
                    Index index(new FamIndex(s, rec->header_.version_, directory_, absPath, offset));
                    for (const auto& dataURI : index.dataURIs()) { data.insert(dataURI); }
                }
            }
        }
    }
}

size_t FamHandler::FamFilesSize() const {
    // Get the size of the master Fam

    size_t size = famPath().size();

    // If we have subTOCS, we need to get those too!

    std::vector<eckit::PathName> subTOCS = subTocPaths();

    for (std::vector<eckit::PathName>::const_iterator i = subTOCS.begin(); i != subTOCS.end(); ++i) {
        size += i->size();
    }

    return size;
}

// std::string FamHandler::userName(long id) const {
//     struct passwd* p = getpwuid(id);
//     if (p) {
//         return p->pw_name;
//     } else {
//         return eckit::Translator<long, std::string>()(id);
//     }
// }

auto FamHandler::currentRoot() const -> const eckit::FamRegionName& {
    if (subTocRead_) { return subTocRead_->currentRoot(); }
    return root_;
}

const FamObjectName& FamHandler::currentToc() const {
    if (subTocRead_) { return subTocRead_->currentToc(); }
    return handler_;
}

const Key& FamHandler::currentRemapKey() const {
    if (subTocRead_) { return subTocRead_->currentRemapKey(); }
    return remapKey_;
}

size_t FamHandler::buildIndexRecord(FamRecord& rec, const Index& index) {
    const IndexLocation&    location(index.location());
    const FamIndexLocation& FamLoc(reinterpret_cast<const FamIndexLocation&>(location));

    ASSERT(rec.header_.tag_ == FamRecord::Tag::TOC_INDEX);

    eckit::MemoryStream s(&rec.payload_[0], rec.capacity);

    s << FamLoc.uri().path().baseName();
    s << FamLoc.offset();
    s << index.type();
    index.encode(s, rec.header_.version_);

    return s.position();
}

size_t FamHandler::buildClearRecord(FamRecord& rec, const Index& index) {
    struct FamIndexLocationExtracter: public IndexLocationVisitor {
        FamIndexLocationExtracter(FamRecord& rec): r_(rec), sz_(0) { }

        virtual void operator()(const IndexLocation& l) {
            const FamIndexLocation& location = reinterpret_cast<const FamIndexLocation&>(l);

            eckit::MemoryStream s(&r_.payload_[0], r_.capacity);

            s << location.uri().path().baseName();
            s << location.offset();
            ASSERT(sz_ == 0);
            sz_ = s.position();
            LOG_DEBUG_LIB(LibFdb5) << "Write Fam_CLEAR " << location.uri().path().baseName() << " - "
                                   << location.offset() << std::endl;
        }

        size_t size() const { return sz_; }

    private:
        FamRecord& r_;
        size_t     sz_;
    };

    ASSERT(rec.header_.tag_ == FamRecord::Tag::TOC_CLEAR);
    FamIndexLocationExtracter visitor(rec);
    index.visit(visitor);
    return visitor.size();
}

size_t FamHandler::buildSubTocMaskRecord(FamRecord& rec) {
    /// n.b. We construct a subtoc masking record using Fam_CLEAR for backward compatibility.

    ASSERT(useSubToc_);
    ASSERT(subTocWrite_);

    // We use a relative path to this subtoc if it belongs to the current DB
    // but an absolute one otherwise (e.g. for fdb-overlay).
    const eckit::PathName& absPath = subTocWrite_->famPath();
    eckit::PathName        path    = (absPath.dirName().sameAs(directory_)) ? absPath.baseName() : absPath;

    return buildSubTocMaskRecord(rec, path);
}

size_t FamHandler::buildSubTocMaskRecord(FamRecord& rec, const eckit::PathName& path) {
    /// n.b. We construct a subtoc masking record using Fam_CLEAR for backward compatibility.

    ASSERT(rec.header_.tag_ == FamRecord::Tag::TOC_CLEAR);

    eckit::MemoryStream s(&rec.payload_[0], rec.capacity);

    s << path;
    s << static_cast<off_t>(0);  // Always use an offset of zero for subTOCS

    return s.position();
}

void FamHandler::control(const ControlAction& action, const ControlIdentifiers& identifiers) const {
    for (auto&& identifier : identifiers) {
        auto it = controlfile_lookup.find(identifier);
        ASSERT(it != controlfile_lookup.end());

        const std::string& lock_file(it->second);

        switch (action) {
            case ControlAction::Disable: createControlFile(lock_file); break;

            case ControlAction::Enable:  removeControlFile(lock_file); break;

            default:                     eckit::Log::warning() << "Unexpected action: " << static_cast<uint16_t>(action) << std::endl;
        }
    }
}

bool FamHandler::enabled(const ControlIdentifier& controlIdentifier) const {
    auto it = controlfile_lookup.find(controlIdentifier);
    ASSERT(it != controlfile_lookup.end());

    const std::string& control_file(it->second);
    return !fullControlFilePath(control_file).exists();
};

std::vector<PathName> FamHandler::lockfilePaths() const {
    std::vector<PathName> paths;

    for (const auto& name : {retrieve_lock_file, archive_lock_file, list_lock_file, wipe_lock_file}) {
        eckit::PathName fullPath = fullControlFilePath(name);
        if (fullPath.exists()) { paths.emplace_back(std::move(fullPath)); }
    }

    return paths;
}

PathName FamHandler::fullControlFilePath(const std::string& name) const {
    root_.object(name);
    // return directory_ / name;
}

void FamHandler::createControlFile(const std::string& name) const {
    // It is not an error to lock something that is already locked
    eckit::PathName fullPath(fullControlFilePath(name));
    if (!fullPath.exists()) { fullPath.touch(); }
}

void FamHandler::removeControlFile(const std::string& name) const {
    // It is not an error to unlock something that is already unlocked
    eckit::PathName fullPath(fullControlFilePath(name));
    if (fullPath.exists()) {
        bool verbose = false;
        fullPath.unlink(verbose);
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

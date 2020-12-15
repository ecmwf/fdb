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
#include <sys/types.h>
#include <pwd.h>

#include "eckit/config/Resource.h"
#include "eckit/io/FileHandle.h"
#include "eckit/io/FileDescHandle.h"
#include "eckit/log/BigNum.h"
#include "eckit/log/Log.h"
#include "eckit/maths/Functions.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/StaticMutex.h"
#include "eckit/filesystem/PathName.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Index.h"
#include "fdb5/toc/TocCommon.h"
#include "fdb5/toc/TocFieldLocation.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/TocStats.h"
#include "fdb5/api/helpers/ControlIterator.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {
    constexpr const char* retrieve_lock_file = "retrieve.lock";
    constexpr const char* archive_lock_file = "archive.lock";
    constexpr const char* list_lock_file = "list.lock";
    constexpr const char* wipe_lock_file = "wipe.lock";
}

//----------------------------------------------------------------------------------------------------------------------

class TocHandlerCloser {
    const TocHandler& handler_;
  public:
    TocHandlerCloser(const TocHandler &handler): handler_(handler) {}
    ~TocHandlerCloser() {
        handler_.close();
    }
};

//----------------------------------------------------------------------------------------------------------------------

class CachedFDProxy {
public: // methods

    CachedFDProxy(const eckit::PathName& path, int fd, std::unique_ptr<eckit::MemoryHandle>& cached) :
        path_(path),
        fd_(fd),
        cached_(cached.get()) {
        ASSERT((fd != -1) != (!!cached));
    }

    long read(void* buf, long len) {
        if (cached_) {
            return cached_->read(buf, len);
        } else {
            long ret;
            SYSCALL2( ret = ::read(fd_, buf, len), path_);
            return ret;
        }
    }

    Offset position() {
        if (cached_) {
            return cached_->position();
        } else {
            off_t pos;
            SYSCALL(pos = ::lseek(fd_, 0, SEEK_CUR));
            return pos;
        }
    }

    Offset seek(const Offset& pos) {
        if (cached_) {
            return cached_->seek(pos);
        } else {
            off_t ret;
            SYSCALL(ret = ::lseek(fd_, pos, SEEK_SET));
            ASSERT(ret == pos);
            return pos;
        }
    }

private: // members

    const eckit::PathName& path_;
    int fd_;
    MemoryHandle* cached_;
};

//----------------------------------------------------------------------------------------------------------------------

TocHandler::TocHandler(const eckit::PathName& directory, const Config& config) :
    TocCommon(directory),
    tocPath_(directory_ / "toc"),
    dbConfig_(config),
    useSubToc_(config.getBool("useSubToc", false)),
    isSubToc_(false),
    fd_(-1),
    cachedToc_(nullptr),
    count_(0),
    enumeratedMaskedEntries_(false),
    writeMode_(false)
{

    // An override to enable using sub tocs without configurations being passed in, for ease
    // of debugging
    const char* subTocOverride = ::getenv("FDB5_SUB_TOCS");
    if (subTocOverride) {
        useSubToc_ = true;
    }
}

TocHandler::TocHandler(const eckit::PathName& path, const Key& parentKey) :
    TocCommon(path.dirName()),
    parentKey_(parentKey),
    tocPath_(TocCommon::findRealPath(path)),
    useSubToc_(false),
    isSubToc_(true),
    fd_(-1),
    cachedToc_(nullptr),
    count_(0),
    enumeratedMaskedEntries_(false),
    writeMode_(false)
{

    /// Are we remapping a mounted DB?
    if (exists()) {
        Key key(databaseKey());
        if (!parentKey.empty() && parentKey != key) {

            eckit::Log::debug<LibFdb5>() << "Opening (remapped) toc with differing parent key: "
                                         << key << " --> " << parentKey << std::endl;

            if (parentKey.size() != key.size()) {
                std::stringstream ss;
                ss << "Keys insufficiently matching for mount: " << key << " : " << parentKey;
                throw UserError(ss.str(), Here());
            }

            for (const auto& kv : parentKey) {
                auto it = key.find(kv.first);
                if (it == key.end()) {
                    std::stringstream ss;
                    ss << "Keys insufficiently matching for mount: " << key << " : " << parentKey;
                    throw UserError(ss.str(), Here());
                } else if (kv.second != it->second) {
                    remapKey_.set(kv.first, kv.second);
                }
            }

            eckit::Log::debug<LibFdb5>() << "Key remapping: " << remapKey_ << std::endl;
        }
    }
}


TocHandler::~TocHandler() {
    close();
}

bool TocHandler::exists() const {
    return tocPath_.exists();
}

void TocHandler::checkUID() const {
    static bool fdbOnlyCreatorCanWrite = eckit::Resource<bool>("fdbOnlyCreatorCanWrite", true);
    if (!fdbOnlyCreatorCanWrite) {
        return;
    }

    static std::vector<std::string> fdbSuperUsers = eckit::Resource<std::vector<std::string> >("fdbSuperUsers", "", true);

    if (dbUID() != userUID_) {

        if(std::find(fdbSuperUsers.begin(), fdbSuperUsers.end(), userName(userUID_)) == fdbSuperUsers.end()) {

            std::ostringstream oss;
            oss << "Only user '"
                << userName(dbUID())

                << "' can write to FDB "
                << directory_
                << ", current user is '"
                << userName(userUID_)
                << "'";

            throw eckit::UserError(oss.str());
        }
    }
}

void TocHandler::openForAppend() {

    checkUID(); // n.b. may openForRead

    if (cachedToc_) {
        cachedToc_.reset();
    }

    writeMode_ = true;

    ASSERT(fd_ == -1);

    eckit::Log::debug<LibFdb5>() << "Opening for append TOC " << tocPath_ << std::endl;

    int iomode = O_WRONLY | O_APPEND;
#ifdef O_NOATIME
    // this introduces issues of permissions
    static bool fdbNoATime = eckit::Resource<bool>("fdbNoATime;$FDB_OPEN_NOATIME", false);
    if(fdbNoATime) {
        iomode |= O_NOATIME;
    }
#endif
    SYSCALL2((fd_ = ::open( tocPath_.localPath(), iomode, (mode_t)0777 )), tocPath_);
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

    eckit::Log::debug<LibFdb5>() << "Opening for read TOC " << tocPath_ << std::endl;

    int iomode = O_RDONLY;
#ifdef O_NOATIME
    // this introduces issues of permissions
    static bool fdbNoATime = eckit::Resource<bool>("fdbNoATime;$FDB_OPEN_NOATIME", false);
    if(fdbNoATime) {
        iomode |= O_NOATIME;
    }
#endif
    SYSCALL2((fd_ = ::open( tocPath_.localPath(), iomode )), tocPath_ );

    // The masked subtocs and indexes could be updated each time, so reset this.
    enumeratedMaskedEntries_ = false;
    maskedEntries_.clear();

    if(fdbCacheTocsOnRead) {

        FileDescHandle toc(fd_, true); // closes the file descriptor
        AutoClose closer1(toc);
        fd_ = -1;

        bool grow = true;
        cachedToc_.reset( new eckit::MemoryHandle(tocPath().size(), grow) );

        long buffersize = 4*1024*1024;
        toc.copyTo(*cachedToc_, buffersize);

        cachedToc_->openForRead();
    }
}

void TocHandler::append(TocRecord &r, size_t payloadSize ) {

    ASSERT(fd_ != -1);
    ASSERT(not cachedToc_);

    Log::debug<LibFdb5>() << "Writing toc entry: " << (int)r.header_.tag_ << std::endl;

    // Obtain the rounded size, and set it in the record header.
    size_t roundedSize = roundRecord(r, payloadSize);

    size_t len;
    SYSCALL2( len = ::write(fd_, &r, roundedSize), tocPath_ );
    dirty_ = true;
    ASSERT( len == roundedSize);
}

void TocHandler::appendBlock(const void *data, size_t size) {

    openForAppend();
    TocHandlerCloser close(*this);

    ASSERT(fd_ != -1);
    ASSERT(not cachedToc_);

    // Ensure that this block is appropriately rounded.

    ASSERT(size % recordRoundSize() == 0);

    size_t len;
    SYSCALL2( len = ::write(fd_, data, size), tocPath_ );
    dirty_ = true;
    ASSERT( len == size );
}

size_t TocHandler::recordRoundSize() {

    static size_t fdbRoundTocRecords = eckit::Resource<size_t>("fdbRoundTocRecords", 1024);
    return fdbRoundTocRecords;
}

size_t TocHandler::roundRecord(TocRecord &r, size_t payloadSize) {

    r.header_.size_ = eckit::round(sizeof(TocRecord::Header) + payloadSize, recordRoundSize());

    return r.header_.size_;
}

// readNext wraps readNextInternal.
// readNext reads the next TOC entry from this toc, or from an appropriate subtoc if necessary.
bool TocHandler::readNext( TocRecord &r, bool walkSubTocs, bool hideSubTocEntries, bool hideClearEntries) const {

    int len;

    // Ensure we are able to skip masked entries as appropriate

    if (!enumeratedMaskedEntries_) {
        populateMaskedEntriesList();
    }

    // For some tools (mainly diagnostic) it makes sense to be able to switch the
    // walking behaviour here.

    if (!walkSubTocs)
        return readNextInternal(r);

    while (true) {

        if (subTocRead_) {
            len = subTocRead_->readNext(r, walkSubTocs, hideSubTocEntries, hideClearEntries);
            if (len == 0) {
                subTocRead_.reset();
            } else {
                ASSERT(r.header_.tag_ != TocRecord::TOC_SUB_TOC);
                return true;
            }
        } else {

            if (!readNextInternal(r)) {

                return false;

            } else if (r.header_.tag_ == TocRecord::TOC_INIT) {

                eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
                if (parentKey_.empty()) parentKey_ = Key(s);
                return true;

            } else if (r.header_.tag_ == TocRecord::TOC_SUB_TOC) {

                eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
                eckit::PathName path;
                s >> path;

                // Handle both path and absPath for compatibility as we move from storing
                // absolute paths to relative paths. Either may exist in either the TOC_SUB_TOC
                // or TOC_CLEAR entries.
                ASSERT(path.path().size() > 0);
                eckit::PathName absPath = (path.path()[0] == '/') ? findRealPath(path) : (currentDirectory() / path);

                // If this subtoc has a masking entry, then skip it, and go on to the next entry.
                std::pair<eckit::PathName, size_t> key(absPath, 0);
                if (maskedEntries_.find(key) != maskedEntries_.end()) {
                    Log::debug<LibFdb5>() << "SubToc ignored by mask: " << path << std::endl;
                    continue;
                }

                eckit::Log::debug<LibFdb5>() << "Opening SUB_TOC: " << absPath << " " << parentKey_ << std::endl;

                subTocRead_.reset(new TocHandler(absPath, parentKey_));
                subTocRead_->openForRead();

                if (hideSubTocEntries) {
                    // The first entry in a subtoc must be the init record. Check that
                    subTocRead_->readNext(r, walkSubTocs, hideSubTocEntries, hideClearEntries);
                    ASSERT(r.header_.tag_ == TocRecord::TOC_INIT);
                } else {
                    return true; // if not hiding the subtoc entries, return them as normal entries!
                }

            } else if (r.header_.tag_ == TocRecord::TOC_INDEX) {

                eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
                eckit::PathName path;
                off_t offset;
                s >> path;
                s >> offset;

                PathName absPath = currentDirectory() / path;

                std::pair<eckit::PathName, size_t> key(absPath, offset);
                if (maskedEntries_.find(key) != maskedEntries_.end()) {
                    Log::debug<LibFdb5>() << "Index ignored by mask: " << path << ":" << offset << std::endl;
                    continue;
                }

                return true;

            } else if (r.header_.tag_ == TocRecord::TOC_CLEAR && hideClearEntries) {
                continue; // we already handled the TOC_CLEAR entries in populateMaskedEntriesList()
            } else {
                // A normal read operation
                return true;
            }
        }
    }
}

static void checkSupportedVersion(unsigned int toc_header_version) {
    static std::vector<unsigned int> supported = LibFdb5::instance().supportedSerialisationVersions();
    for (auto version: supported) {
        if (toc_header_version == version) return;
    }
    std::ostringstream oss;
    oss << "Record version mistach, software supports versions " << supported << " got " << toc_header_version;
    throw eckit::SeriousBug(oss.str());
}

// readNext wraps readNextInternal.
// readNextInternal reads the next TOC entry from this toc.
bool TocHandler::readNextInternal(TocRecord& r) const {

    CachedFDProxy proxy(tocPath_, fd_, cachedToc_);

    long len = proxy.read(&r, sizeof(TocRecord::Header));
    if (len == 0) {
        return false;
    }
    ASSERT(len == sizeof(TocRecord::Header));

    len = proxy.read(&r.payload_, r.header_.size_ - sizeof(TocRecord::Header));
    ASSERT(size_t(len) == r.header_.size_ - sizeof(TocRecord::Header));

    checkSupportedVersion(r.header_.version_);

    return true;
}

std::vector<PathName> TocHandler::subTocPaths() const {

    openForRead();
    TocHandlerCloser close(*this);

    std::unique_ptr<TocRecord> r(new TocRecord); // allocate (large) TocRecord on heap not stack (MARS-779)

    std::vector<eckit::PathName> paths;

    bool walkSubTocs = true;
    bool hideSubTocEntries = false;
    bool hideClearEntries = true;
    while ( readNext(*r, walkSubTocs, hideSubTocEntries, hideClearEntries) ) {

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
                ASSERT_MSG(r->header_.tag_ != TocRecord::TOC_CLEAR, "The TOC_CLEAR records should have been pre-filtered on the first pass");
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
        subTocRead_.reset();
    }

    if (subTocWrite_) {
        // We keep track of the sub toc we are writing to until the process is closed, so don't reset
        // the pointer here (or we will create a proliferation of sub tocs)
        subTocWrite_->close();
    }

    if ( fd_ >= 0 ) {
        eckit::Log::debug<LibFdb5>() << "Closing TOC " << tocPath_ << std::endl;
        if(dirty_) {
            SYSCALL2( eckit::fdatasync(fd_), tocPath_ );
            dirty_ = false;
        }
        SYSCALL2( ::close(fd_), tocPath_ );
        fd_ = -1;
        writeMode_ = false;
    }
}

void TocHandler::allMaskableEntries(Offset startOffset, Offset endOffset,
                                    std::set<std::pair<PathName, Offset>>& entries) const {

    CachedFDProxy proxy(tocPath_, fd_, cachedToc_);

    // Start reading entries where we are told to

    Offset ret = proxy.seek(startOffset);
    ASSERT(ret == startOffset);

    std::unique_ptr<TocRecord> r(new TocRecord); // allocate (large) TocRecord on heap not stack (MARS-779)

    while (proxy.position() < endOffset) {

        ASSERT(readNextInternal(*r));

        eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);
        std::string path;
        off_t offset;

        switch (r->header_.tag_) {
            case TocRecord::TOC_SUB_TOC: {
                s >> path;
                ASSERT(path.size() > 0);
                eckit::PathName absPath = (path[0] == '/') ? findRealPath(path) : (currentDirectory() / path);
                entries.emplace(std::pair<PathName, Offset>(absPath, 0));
                break;
	    }

            case TocRecord::TOC_INDEX:
                s >> path;
                s >> offset;
                // readNextInternal --> use directory_ not currentDirectory()
                entries.emplace(std::pair<PathName, Offset>(directory_ / path, offset));
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

void TocHandler::populateMaskedEntriesList() const {

    ASSERT(fd_ != -1 || cachedToc_);
    CachedFDProxy proxy(tocPath_, fd_, cachedToc_);

    Offset startPosition = proxy.position(); // remember the current position of the file descriptor

    maskedEntries_.clear();

    std::unique_ptr<TocRecord> r(new TocRecord); // allocate (large) TocRecord on heap not stack (MARS-779)

    while ( readNextInternal(*r) ) {

        eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);
        std::string path;
        off_t offset;

        switch (r->header_.tag_) {

            case TocRecord::TOC_CLEAR: {
                s >> path;
                s >> offset;

                if (path == "*") { // For the "*" path, mask EVERYTHING that we have already seen
                    Offset currentPosition = proxy.position();
                    allMaskableEntries(startPosition, currentPosition, maskedEntries_);
                    ASSERT(currentPosition == proxy.position());
                } else {
                    // readNextInternal --> use directory_ not currentDirectory()
                    ASSERT(path.size() > 0);
                    eckit::PathName absPath = (path[0] == '/') ? findRealPath(path) : (directory_ / path);
                    maskedEntries_.emplace(std::pair<PathName, Offset>(absPath, offset));
                }
                break;
            }

            case TocRecord::TOC_SUB_TOC:
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

    enumeratedMaskedEntries_ = true;
}

static eckit::StaticMutex local_mutex;

void TocHandler::writeInitRecord(const Key &key) {

    eckit::AutoLock<eckit::StaticMutex> lock(local_mutex);

    if ( !directory_.exists() ) {
        directory_.mkdir();
    }

    // enforce lustre striping if requested
    if (stripeLustre() && !tocPath_.exists()) {
        LustreStripe stripe = stripeIndexLustreSettings();
        fdb5LustreapiFileCreate(tocPath_.localPath(), stripe.size_, stripe.count_);
    }

    ASSERT(fd_ == -1);

    int iomode = O_CREAT | O_RDWR;
    SYSCALL2(fd_ = ::open( tocPath_.localPath(), iomode, mode_t(0777) ), tocPath_);

    TocHandlerCloser closer(*this);

    std::unique_ptr<TocRecord> r(new TocRecord); // allocate (large) TocRecord on heap not stack (MARS-779)

    size_t len = readNext(*r);
    if (len == 0) {

        eckit::Log::debug<LibFdb5>() << "Initializing FDB TOC in " << tocPath_ << std::endl;

        if (!isSubToc_) {

            /* Copy schema first */

            eckit::Log::debug<LibFdb5>() << "Copy schema from "
                               << dbConfig_.schemaPath()
                               << " to "
                               << schemaPath_
                               << std::endl;

            eckit::PathName tmp = eckit::PathName::unique(schemaPath_);

            eckit::FileHandle in(dbConfig_.schemaPath());

            // Enforce lustre striping if requested

            // SDS: Would be nicer to do this, but FileHandle doesn't have a path_ member, let alone an exposed one
            //      so would need some tinkering to work with LustreFileHandle.
            // LustreFileHandle<eckit::FileHandle> out(tmp, stripeIndexLustreSettings());

            if (stripeLustre()) {
                LustreStripe stripe = stripeIndexLustreSettings();
                fdb5LustreapiFileCreate(tmp.localPath(), stripe.size_, stripe.count_);
            }
            eckit::FileHandle out(tmp);
            in.copyTo(out);

            eckit::PathName::rename(tmp, schemaPath_);
        }

        std::unique_ptr<TocRecord> r2(new TocRecord(TocRecord::TOC_INIT)); // allocate TocRecord on heap (MARS-779)
        eckit::MemoryStream s(&r2->payload_[0], r2->maxPayloadSize);
        s << key;
        s << isSubToc_;
        append(*r2, s.position());
        dbUID_ = r2->header_.uid_;

    } else {
        ASSERT(r->header_.tag_ == TocRecord::TOC_INIT);
        eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);
        ASSERT(key == Key(s));
        dbUID_ = r->header_.uid_;
    }
}

void TocHandler::writeClearRecord(const Index &index) {

    std::unique_ptr<TocRecord> r(new TocRecord(TocRecord::TOC_CLEAR)); // allocate (large) TocRecord on heap not stack (MARS-779)

    size_t sz = roundRecord(*r, buildClearRecord(*r, index));
    appendBlock(r.get(), sz);
}

void TocHandler::writeClearAllRecord() {

    std::unique_ptr<TocRecord> r(new TocRecord(TocRecord::TOC_CLEAR)); // allocate (large) TocRecord on heap not stack (MARS-779)

    eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);
    s << std::string {"*"};
    s << off_t{0};

    size_t sz = roundRecord(*r, s.position());
    appendBlock(r.get(), sz);
}

void TocHandler::writeSubTocRecord(const TocHandler& subToc) {

    openForAppend();
    TocHandlerCloser closer(*this);

    std::unique_ptr<TocRecord> r(new TocRecord(TocRecord::TOC_SUB_TOC)); // allocate (large) TocRecord on heap not stack (MARS-779)

    // We use a relative path to this subtoc if it belongs to the current DB
    // but an absolute one otherwise (e.g. for fdb-overlay).

    const PathName& absPath = subToc.tocPath();
    // TODO: See FDB-142. Write subtocs as relative.
    // PathName path = (absPath.dirName().sameAs(directory_)) ? absPath.baseName() : absPath;
    const PathName& path = absPath;

    eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);
    s << path;
    s << off_t{0};
    append(*r, s.position());

    eckit::Log::debug<LibFdb5>() << "Write TOC_SUB_TOC " << path << std::endl;
}


void TocHandler::writeIndexRecord(const Index& index) {

    // Toc index writer

    struct WriteToStream : public IndexLocationVisitor {
        WriteToStream(const Index& index, TocHandler& handler) : index_(index), handler_(handler) {}

        virtual void operator() (const IndexLocation& l) {

            const TocIndexLocation& location = reinterpret_cast<const TocIndexLocation&>(l);

            std::unique_ptr<TocRecord> r(new TocRecord(TocRecord::TOC_INDEX)); // allocate (large) TocRecord on heap not stack (MARS-779)

            eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);

            s << location.uri().path().baseName();
            s << location.offset();
            s << index_.type();

            index_.encode(s, r->writeVersion());
            handler_.append(*r, s.position());

            eckit::Log::debug<LibFdb5>() << "Write TOC_INDEX " << location.uri().path().baseName() << " - " << location.offset() << " " << index_.type() << std::endl;
        }

    private:
        const Index& index_;
        TocHandler& handler_;
    };

    // If we are using a sub toc, delegate there

    if (useSubToc_) {

        // Create the sub toc, and insert the redirection record into the the master toc.

        if (!subTocWrite_) {

            subTocWrite_.reset(new TocHandler(eckit::PathName::unique(tocPath_), Key{}));

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

void TocHandler::writeSubTocMaskRecord(const TocHandler &subToc) {

    std::unique_ptr<TocRecord> r(new TocRecord(TocRecord::TOC_CLEAR)); // allocate (large) TocRecord on heap not stack (MARS-779)

    // We use a relative path to this subtoc if it belongs to the current DB
    // but an absolute one otherwise (e.g. for fdb-overlay).

    const PathName& absPath = subToc.tocPath();
    // TODO: See FDB-142. Write subtocs as relative.
    // PathName path = (absPath.dirName().sameAs(directory_)) ? absPath.baseName() : absPath;
    const PathName& path = absPath;

    size_t sz = roundRecord(*r, buildSubTocMaskRecord(*r, path));
    appendBlock(r.get(), sz);
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
    HasPath(const eckit::PathName &path, off_t offset): path_(path), offset_(offset) {}
    bool operator()(const Index index) const {

        const TocIndex* tocidx = dynamic_cast<const TocIndex*>(index.content());

        if(!tocidx) {
            throw eckit::NotImplemented("Index is not of TocIndex type -- referencing unknown Index types isn't supported", Here());
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

    // Allocate (large) TocRecord on heap not stack (MARS-779)
    std::unique_ptr<TocRecord> r(new TocRecord);

    while ( readNext(*r) ) {
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

    // Allocate (large) TocRecord on heap not stack (MARS-779)
    std::unique_ptr<TocRecord> r(new TocRecord);

    while ( readNext(*r) ) {
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

        // Allocate (large) TocRecord on heap not stack (MARS-779)
        std::unique_ptr<TocRecord> r(new TocRecord);

        bool walkSubTocs = true;
        bool hideSubTocEntries = false;
        bool hideClearEntries = false;
        while ( readNext(*r, walkSubTocs, hideSubTocEntries, hideClearEntries) ) {
            count_++;
        }
    }

    return count_;
}

const eckit::PathName& TocHandler::directory() const
{
    return directory_;
}

std::vector<Index> TocHandler::loadIndexes(bool sorted,
                                           std::set<std::string>* subTocs,
                                           std::vector<bool>* indexInSubtoc,
                                           std::vector<Key>* remapKeys) const {

    std::vector<Index> indexes;

    if (!tocPath_.exists()) {
        return indexes;
    }

    openForRead();
    TocHandlerCloser close(*this);

    // Allocate (large) TocRecord on heap not stack (MARS-779)
    std::unique_ptr<TocRecord> r(new TocRecord);
    count_ = 0;

    bool debug = LibFdb5::instance().debug();
    bool walkSubTocs = true;
    bool hideSubTocEntries = true;
    bool hideClearEntries = true;
    while ( readNext(*r, walkSubTocs, hideSubTocEntries, hideClearEntries) ) {

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
            s >> path;
            s >> offset;
            s >> type;
            LOG_DEBUG(debug, LibFdb5) << "TocRecord TOC_INDEX " << path << " - " << offset << std::endl;
            indexes.push_back( new TocIndex(s, r->header_.version_, currentDirectory(), currentDirectory() / path, offset) );

            if (subTocs != 0 && subTocRead_) {
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
           ASSERT_MSG(r->header_.tag_ != TocRecord::TOC_CLEAR, "The TOC_CLEAR records should have been pre-filtered on the first pass");
            break;

        case TocRecord::TOC_SUB_TOC:
            throw eckit::SeriousBug("TOC_SUB_TOC entry should be handled inside readNext");
            break;

        default:
            std::ostringstream oss;
            oss << "Unknown tag in TocRecord " << *r;
            throw eckit::SeriousBug(oss.str(), Here());
            break;

        }

    }

    // For some purposes, it is useful to have the indexes sorted by their location, as this is is faster for
    // iterating through the data.

    if (sorted) {

        ASSERT(!indexInSubtoc);
        ASSERT(!remapKeys);
        std::sort(indexes.begin(), indexes.end(), TocIndexFileSort());

    } else {

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

const eckit::PathName &TocHandler::tocPath() const {
    return tocPath_;
}

const eckit::PathName &TocHandler::schemaPath() const {
    return schemaPath_;
}


void TocHandler::dump(std::ostream& out, bool simple, bool walkSubTocs) const {

    openForRead();
    TocHandlerCloser close(*this);

    // Allocate (large) TocRecord on heap not stack (MARS-779)
    std::unique_ptr<TocRecord> r(new TocRecord);

    bool hideSubTocEntries = false;
    bool hideClearEntries = false;
    while ( readNext(*r, walkSubTocs, hideSubTocEntries, hideClearEntries) ) {

        eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);
        std::string path;
        std::string type;
        bool isSubToc;

        off_t offset;
        std::vector<Index>::iterator j;

        r->dump(out, simple);

        switch (r->header_.tag_) {

            case TocRecord::TOC_INIT: {
                isSubToc = false;
                fdb5::Key key(s);
                if (r->header_.version_ > 1) {
                    s >> isSubToc;
                }
                out << "  Key: " << key << ", sub-toc: " << (isSubToc ? "yes" : "no");
                if(!simple) { out << std::endl; }
                break;
            }

            case TocRecord::TOC_INDEX: {
                s >> path;
                s >> offset;
                s >> type;
                out << "  Path: " << path << ", offset: " << offset << ", type: " << type;
                if(!simple) { out << std::endl; }
                Index index(new TocIndex(s, r->header_.version_, currentDirectory(), currentDirectory() / path, offset));
                index.dump(out, "  ", simple);
                break;
            }

            case TocRecord::TOC_CLEAR: {
                s >> path;
                s >> offset;
                out << "  Path: " << path << ", offset: " << offset;
                break;
            }

            case TocRecord::TOC_SUB_TOC: {
                s >> path;
                out << "  Path: " << path;
                break;
            }

            default: {
                out << "   Unknown TOC entry";
                break;
            }
        }
        out << std::endl;
    }
}


void TocHandler::dumpIndexFile(std::ostream& out, const eckit::PathName& indexFile) const {

    openForRead();
    TocHandlerCloser close(*this);

    // Allocate (large) TocRecord on heap not stack (MARS-779)
    std::unique_ptr<TocRecord> r(new TocRecord);

    bool walkSubTocs = true;
    bool hideSubTocEntries = true;
    bool hideClearEntries = true;
    while ( readNext(*r, walkSubTocs, hideSubTocEntries, hideClearEntries) ) {

        eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);
        std::string path;
        std::string type;
        off_t offset;

        switch (r->header_.tag_) {

            case TocRecord::TOC_INDEX: {
                s >> path;
                s >> offset;
                s >> type;

                if ((currentDirectory() / path).fullName() == indexFile.fullName()) {
                    out << "  Path: " << path << ", offset: " << offset << ", type: " << type;
                    Index index(new TocIndex(s, r->header_.version_, currentDirectory(), currentDirectory() / path, offset));
                    index.dump(out, "  ", false, true);
                }
                break;
            }

            case TocRecord::TOC_SUB_TOC:
            case TocRecord::TOC_CLEAR:
                ASSERT_MSG(r->header_.tag_ != TocRecord::TOC_CLEAR, "The TOC_CLEAR records should have been pre-filtered on the first pass");
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

DbStats TocHandler::stats() const
{
    TocDbStats* stats = new TocDbStats();

    stats->dbCount_         += 1;
    stats->tocRecordsCount_ += numberOfRecords();
    stats->tocFileSize_     += tocFilesSize();
    stats->schemaFileSize_  += schemaPath().size();

    return DbStats(stats);
}


void TocHandler::enumerateMasked(std::set<std::pair<eckit::URI, Offset>>& metadata,
                                 std::set<eckit::URI>& data) const {

    if (!enumeratedMaskedEntries_) {
        populateMaskedEntriesList();
    }

    for (const auto& entry : maskedEntries_) {

        eckit::URI uri("toc", entry.first);
        if (entry.first.exists()) {
            metadata.insert(std::make_pair(uri, entry.second));

            // If this is a subtoc, then enumerate its contained indexes and data!

            if (uri.path().baseName().asString().substr(0, 4) == "toc.") {
                TocHandler h(uri.path(), remapKey_);

                h.enumerateMasked(metadata, data);

                std::vector<Index> indexes = h.loadIndexes();
                for (const auto& i : indexes) {
                    metadata.insert(std::make_pair<eckit::URI, Offset>(i.location().uri(), 0));
                    for (const auto& dataPath : i.dataPaths()) {
                        data.insert(dataPath);
                    }
                }
            }
        }
    }

    // Get the data files referenced by the masked indexes (those in subtocs are
    // referenced internally)

    openForRead();
    TocHandlerCloser close(*this);

    // Allocate (large) TocRecord on heap not stack (MARS-779)
    std::unique_ptr<TocRecord> r(new TocRecord);

    while ( readNextInternal(*r) ) {
        if (r->header_.tag_ == TocRecord::TOC_INDEX) {

            eckit::MemoryStream s(&r->payload_[0], r->maxPayloadSize);

            std::string path;
            std::string type;
            off_t offset;
            s >> path;
            s >> offset;
            s >> type;

            // n.b. readNextInternal --> directory_ not currentDirectory()
            PathName absPath = directory_ / path;

            std::pair<eckit::PathName, size_t> key(absPath, offset);
            if (maskedEntries_.find(key) != maskedEntries_.end()) {
                if (absPath.exists()) {
                    Index index(new TocIndex(s, r->header_.version_, directory_, absPath, offset));
                    for (const auto& dataPath : index.dataPaths()) data.insert(dataPath);
                }
            }
        }
    }
}


size_t TocHandler::tocFilesSize() const {

    // Get the size of the master toc

    size_t size =  tocPath().size();

    // If we have subtocs, we need to get those too!

    std::vector<eckit::PathName> subtocs = subTocPaths();

    for (std::vector<eckit::PathName>::const_iterator i = subtocs.begin(); i != subtocs.end(); ++i) {
        size += i->size();
    }

    return size;
}

std::string TocHandler::userName(long id) const {
  struct passwd *p = getpwuid(id);

  if (p) {
    return p->pw_name;
  } else {
    return eckit::Translator<long, std::string>()(id);
  }
}

bool TocHandler::stripeLustre() {
    static bool handleLustreStripe = eckit::Resource<bool>("fdbHandleLustreStripe;$FDB_HANDLE_LUSTRE_STRIPE", true);
    return fdb5LustreapiSupported() && handleLustreStripe;
}


LustreStripe TocHandler::stripeIndexLustreSettings() {

    static unsigned int fdbIndexLustreStripeCount = eckit::Resource<unsigned int>("fdbIndexLustreStripeCount;$FDB_INDEX_LUSTRE_STRIPE_COUNT", 1);
    static size_t fdbIndexLustreStripeSize = eckit::Resource<size_t>("fdbIndexLustreStripeSize;$FDB_INDEX_LUSTRE_STRIPE_SIZE", 8*1024*1024);

    return LustreStripe(fdbIndexLustreStripeCount, fdbIndexLustreStripeSize);
}


LustreStripe TocHandler::stripeDataLustreSettings() {

    static unsigned int fdbDataLustreStripeCount = eckit::Resource<unsigned int>("fdbDataLustreStripeCount;$FDB_DATA_LUSTRE_STRIPE_COUNT", 8);
    static size_t fdbDataLustreStripeSize = eckit::Resource<size_t>("fdbDataLustreStripeSize;$FDB_DATA_LUSTRE_STRIPE_SIZE", 8*1024*1024);

    return LustreStripe(fdbDataLustreStripeCount, fdbDataLustreStripeSize);
}

const Key &TocHandler::currentRemapKey() const {
    if (subTocRead_) {
        return subTocRead_->currentRemapKey();
    } else {
        return remapKey_;
    }
}

const PathName &TocHandler::currentDirectory() const {
    if (subTocRead_) {
        return subTocRead_->currentDirectory();
    } else {
        return directory_;
    }
}

const PathName& TocHandler::currentTocPath() const {
    if (subTocRead_) {
        return subTocRead_->currentTocPath();
    } else {
        return tocPath_;
    }
}

size_t TocHandler::buildIndexRecord(TocRecord& r, const Index &index) {

    const IndexLocation& location(index.location());
    const TocIndexLocation& tocLoc(reinterpret_cast<const TocIndexLocation&>(location));

    ASSERT(r.header_.tag_ == TocRecord::TOC_INDEX);

    eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);

    s << tocLoc.uri().path().baseName();
    s << tocLoc.offset();
    s << index.type();
    index.encode(s, r.writeVersion());

    return s.position();
}

size_t TocHandler::buildClearRecord(TocRecord &r, const Index &index) {

    struct TocIndexLocationExtracter : public IndexLocationVisitor {

        TocIndexLocationExtracter(TocRecord& r) : r_(r), sz_(0) {}

        virtual void operator() (const IndexLocation& l) {

            const TocIndexLocation& location = reinterpret_cast<const TocIndexLocation&>(l);

            eckit::MemoryStream s(&r_.payload_[0], r_.maxPayloadSize);

            s << location.uri().path().baseName();
            s << location.offset();
            ASSERT(sz_ == 0);
            sz_ = s.position();
            eckit::Log::debug<LibFdb5>() << "Write TOC_CLEAR " << location.uri().path().baseName() << " - " << location.offset() << std::endl;
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
    // TODO: See FDB-142. Write subtocs as relative.
    // PathName path = (absPath.dirName().sameAs(directory_)) ? absPath.baseName() : absPath;
    const PathName& path = absPath;

    return buildSubTocMaskRecord(r, path);
}

size_t TocHandler::buildSubTocMaskRecord(TocRecord& r, const eckit::PathName& path) {

    /// n.b. We construct a subtoc masking record using TOC_CLEAR for backward compatibility.

    ASSERT(r.header_.tag_ == TocRecord::TOC_CLEAR);

    eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);

    s << path;
    s << static_cast<off_t>(0);    // Always use an offset of zero for subtocs

    return s.position();
}

void TocHandler::control(const ControlAction& action, const ControlIdentifiers& identifiers) const {

    const std::map<ControlIdentifier, std::string> lockfile_lookup {
        {ControlIdentifier::Retrieve, retrieve_lock_file},
        {ControlIdentifier::Archive, archive_lock_file},
        {ControlIdentifier::List, list_lock_file},
        {ControlIdentifier::Wipe, wipe_lock_file},
    };

    for (ControlIdentifier identifier : identifiers) {

        auto it = lockfile_lookup.find(identifier);
        ASSERT(it != lockfile_lookup.end());

        const std::string& lock_file(it->second);

        switch (action) {
        case ControlAction::Lock:
            createLockFile(lock_file);
            break;

        case ControlAction::Unlock:
            removeLockFile(lock_file);
            break;

        default:
            eckit::Log::warning() << "Unexpected action: "
                                  << static_cast<uint16_t>(action)
                                  << std::endl;
        }
    }
}

bool TocHandler::retrieveLocked() const {
    return fullLockFilePath(retrieve_lock_file).exists();
}

bool TocHandler::archiveLocked() const {
    return fullLockFilePath(archive_lock_file).exists();
}

bool TocHandler::listLocked() const {
    return fullLockFilePath(list_lock_file).exists();
}

bool TocHandler::wipeLocked() const {
    return fullLockFilePath(wipe_lock_file).exists();
}

std::vector<PathName> TocHandler::lockfilePaths() const {

    std::vector<PathName> paths;

    for (const auto& name : { retrieve_lock_file,
                              archive_lock_file,
                              list_lock_file,
                              wipe_lock_file }) {

        PathName fullPath = fullLockFilePath(name);
        if (fullPath.exists()) paths.emplace_back(std::move(fullPath));
    }

    return paths;
}

PathName TocHandler::fullLockFilePath(const std::string& name) const {
    return directory_ / name;
}

void TocHandler::createLockFile(const std::string& name) const {

    checkUID();

    // It is not an error to lock something that is already locked
    PathName fullPath(fullLockFilePath(name));
    if (!fullPath.exists()) {
        fullPath.touch();
    }
}

void TocHandler::removeLockFile(const std::string& name) const {

    checkUID();

    // It is not an error to unlock something that is already unlocked
    PathName fullPath(fullLockFilePath(name));
    if (fullPath.exists()) {
        bool verbose = false;
        fullPath.unlink(verbose);
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

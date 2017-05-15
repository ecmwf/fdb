/*
 i (C) Copyright 1996-2017 ECMWF.
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

#include "eckit/config/Configuration.h"
#include "eckit/config/Resource.h"
#include "eckit/io/FileHandle.h"
#include "eckit/log/BigNum.h"
#include "eckit/log/Log.h"
#include "eckit/maths/Functions.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/StaticMutex.h"
#include "eckit/filesystem/PathName.h"

#include "fdb5/LibFdb.h"
#include "fdb5/config/MasterConfig.h"
#include "fdb5/database/Index.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/TocStats.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

static eckit::StaticMutex local_mutex;


class TocHandlerCloser {
    const TocHandler& handler_;
  public:
    TocHandlerCloser(const TocHandler &handler): handler_(handler) {}
    ~TocHandlerCloser() {
        handler_.close();
    }
};


//----------------------------------------------------------------------------------------------------------------------

TocHandler::TocHandler(const eckit::PathName &directory, const eckit::Configuration& config) :
    directory_(directory),
    dbUID_(-1),
    userUID_(::getuid()),
    tocPath_(directory / "toc"),
    schemaPath_(directory / "schema"),
    fd_(-1),
    count_(0),
    useSubToc_(config.getBool("useSubToc", false)),
    isSubToc_(false) {

    // An override to enable using sub tocs without configurations being passed in, for ease
    // of debugging
    const char* subTocOverride = ::getenv("FDB5_SUB_TOCS");
    if (subTocOverride) {
        useSubToc_ = true;
    }
}

TocHandler::TocHandler(const eckit::PathName& path, bool) :
    directory_(path.dirName()),
    dbUID_(-1),
    userUID_(::getuid()),
    tocPath_(path),
    schemaPath_(path.dirName() / "schema"),
    fd_(-1),
    count_(0),
    useSubToc_(false),
    isSubToc_(true) {}


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

    checkUID();

    ASSERT(fd_ == -1);

    // eckit::Log::info() << "Opening for append TOC " << tocPath_ << std::endl;

    int iomode = O_WRONLY | O_APPEND;
    //#ifdef __linux__
    //  iomode |= O_NOATIME;
    //#endif
    SYSCALL2((fd_ = ::open( tocPath_.localPath(), iomode, (mode_t)0777 )), tocPath_);
}

void TocHandler::openForRead() const {

    ASSERT(fd_ == -1);

    // eckit::Log::info() << "Opening for read TOC " << tocPath_ << std::endl;

    int iomode = O_RDONLY;
    //#ifdef __linux__
    //  iomode |= O_NOATIME;
    //#endif
    SYSCALL2((fd_ = ::open( tocPath_.localPath(), iomode )), tocPath_ );
}

void TocHandler::append(TocRecord &r, size_t payloadSize ) {

    ASSERT(fd_ != -1);

    static size_t fdbRoundTocRecords = eckit::Resource<size_t>("fdbRoundTocRecords", 1024);

    r.header_.size_ = eckit::round(sizeof(TocRecord::Header) + payloadSize, fdbRoundTocRecords);

    size_t len;
    SYSCALL2( len = ::write(fd_, &r, r.header_.size_), tocPath_ );
    ASSERT( len == r.header_.size_ );

}

// readNext wraps readNextInternal.
// readNext reads the next TOC entry from this toc, or from an appropriate subtoc if necessary.
bool TocHandler::readNext( TocRecord &r, bool walkSubTocs ) const {

    int len;

    // For some tools (mainly diagnostic) it makes sense to be able to switch the
    // walking behaviour here.

    if (!walkSubTocs)
        return readNextInternal(r);


    while (true) {

        if (subTocRead_) {
            len = subTocRead_->readNext(r);
            if (len == 0) {
                subTocRead_.reset();
            } else {
                ASSERT(r.header_.tag_ != TocRecord::TOC_SUB_TOC);
                return true;
            }
        } else {

            if (!readNextInternal(r)) {

                return false;

            } else if (r.header_.tag_ == TocRecord::TOC_SUB_TOC) {

                eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
                eckit::PathName path;
                s >> path;

                subTocRead_.reset(new TocHandler(path, true));
                subTocRead_->openForRead();

                // The first entry in a subtoc must be the init record. Check that
                subTocRead_->readNext(r);
                ASSERT(r.header_.tag_ == TocRecord::TOC_INIT);

            } else {

                // A normal read operation
                return true;
            }
        }
    }
}

// readNext wraps readNextInternal.
// readNextInternal reads the next TOC entry from this toc.
bool TocHandler::readNextInternal( TocRecord &r ) const {

    int len;

    SYSCALL2( len = ::read(fd_, &r, sizeof(TocRecord::Header)), tocPath_ );
    if (len == 0) {
        return false;
    }

    ASSERT(len == sizeof(TocRecord::Header));

    SYSCALL2( len = ::read(fd_, &r.payload_, r.header_.size_ - sizeof(TocRecord::Header)), tocPath_ );
    ASSERT(size_t(len) == r.header_.size_ - sizeof(TocRecord::Header));

    if ( TocRecord::currentVersion() < r.header_.version_ ) {
        std::ostringstream oss;
        oss << "Record version mistach, software handles version <= " << TocRecord::currentVersion()
            << ", got " << r.header_.version_;
        throw eckit::SeriousBug(oss.str());
    }

    return true;
}

void TocHandler::close() const {
    if ( fd_ >= 0 ) {
        // eckit::Log::info() << "Closing TOC " << tocPath_ << std::endl;
        SYSCALL2( ::close(fd_), tocPath_ );
        fd_ = -1;
    }
    if (subTocRead_) {
        subTocRead_->close();
        subTocRead_.reset();
    }
    if (subTocWrite_) {
        // We keep track of the sub toc we are writing to until the process is closed, so don't reset
        // the pointer here (or we will create a proliferation of sub tocs)
        subTocWrite_->close();
    }
}

void TocHandler::writeInitRecord(const Key &key) {

    eckit::AutoLock<eckit::StaticMutex> lock(local_mutex);

    if ( !directory_.exists() ) {
        directory_.mkdir();
    }

    // Enforce lustre striping if requested
    if (stripeLustre()) {
        LustreStripe stripe = stripeIndexLustreSettings();
        fdb5LustreapiFileCreate(tocPath_.localPath(), stripe.size_, stripe.count_);
    }

    int iomode = O_CREAT | O_RDWR;
    SYSCALL2(fd_ = ::open( tocPath_.localPath(), iomode, mode_t(0777) ), tocPath_);

    TocHandlerCloser closer(*this);

    TocRecord r;

    size_t len = readNext(r);
    if (len == 0) {

        eckit::Log::info() << "Initializing FDB TOC in " << tocPath_ << std::endl;

        if (!isSubToc_) {

            /* Copy schema first */

            eckit::Log::debug<LibFdb>() << "Copy schema from "
                               << MasterConfig::instance().schemaPath()
                               << " to "
                               << schemaPath_
                               << std::endl;

            eckit::PathName tmp = eckit::PathName::unique(schemaPath_);

            eckit::FileHandle in(MasterConfig::instance().schemaPath());

            // Enforce lustre striping if requested

            // SDS: Would be nicer to do this, but FileHandle doesn't have a path_ member, let alone an exposed one
            //      so would need some tinkering to work with LustreFileHandle.
            // LustreFileHandle<eckit::FileHandle> out(tmp, stripeIndexLustreSettings());

            if (stripeLustre()) {
                LustreStripe stripe = stripeIndexLustreSettings();
                fdb5LustreapiFileCreate(tmp.localPath(), stripe.size_, stripe.count_);
            }
            eckit::FileHandle out(tmp);
            in.saveInto(out);

            eckit::PathName::rename(tmp, schemaPath_);
        }

        TocRecord r( TocRecord::TOC_INIT );
        eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
        s << key;
        s << isSubToc_;
        append(r, s.position());
        dbUID_ = r.header_.uid_;

    } else {
        ASSERT(r.header_.tag_ == TocRecord::TOC_INIT);
        eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
        ASSERT(key == Key(s));
        dbUID_ = r.header_.uid_;
    }
}

void TocHandler::writeClearRecord(const Index &index) {

    openForAppend();
    TocHandlerCloser closer(*this);

    struct WriteToStream : public IndexLocationVisitor {

        WriteToStream(TocHandler& handler) : handler_(handler) {}

        virtual void operator() (const IndexLocation& l) {

            const TocIndexLocation& location = reinterpret_cast<const TocIndexLocation&>(l);

            TocRecord r( TocRecord::TOC_CLEAR );
            eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);

            s << location.path().baseName();
            s << location.offset();
            handler_.append(r, s.position());

            eckit::Log::debug<LibFdb>() << "TOC_CLEAR " << location.path().baseName() << " - " << location.offset() << std::endl;
        }

    private:
        TocHandler& handler_;
    };

    WriteToStream writeVisitor(*this);
    index.visit(writeVisitor);
}

void TocHandler::writeSubTocRecord(const TocHandler& subToc) {

    openForAppend();
    TocHandlerCloser closer(*this);

    TocRecord r( TocRecord::TOC_SUB_TOC );
    eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
    s << subToc.tocPath();
    append(r, s.position());

    eckit::Log::debug<LibFdb>() << "TOC_SUB_TOC " << subToc.tocPath() << std::endl;
}


void TocHandler::writeIndexRecord(const Index& index) {

    // Toc index writer

    struct WriteToStream : public IndexLocationVisitor {
        WriteToStream(const Index& index, TocHandler& handler) : index_(index), handler_(handler) {}

        virtual void operator() (const IndexLocation& l) {

            const TocIndexLocation& location = reinterpret_cast<const TocIndexLocation&>(l);

            TocRecord r( TocRecord::TOC_INDEX );
            eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);

            s << location.path().baseName();
            s << location.offset();
            s << index_.type();

            index_.encode(s);
            handler_.append(r, s.position());

            eckit::Log::debug<LibFdb>() << "TOC_INDEX " << location.path().baseName() << " - " << location.offset() << " " << index_.type() << std::endl;
        }

    private:
        const Index& index_;
        TocHandler& handler_;
    };

    // If we are using a sub toc, delegate there

    if (useSubToc_) {

        // Create the sub toc, and insert the redirection record into the the master toc.

        if (!subTocWrite_) {

            subTocWrite_.reset(new TocHandler(eckit::PathName::unique(tocPath_), true));

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

long TocHandler::dbUID() const {

    if (dbUID_ != -1) {
        return dbUID_;
    }

    openForRead();
    TocHandlerCloser close(*this);

    TocRecord r;

    while ( readNext(r) ) {
        if (r.header_.tag_ == TocRecord::TOC_INIT) {
            dbUID_ = r.header_.uid_;
            return dbUID_;
        }
    }

    throw eckit::SeriousBug("Cannot find a TOC_INIT record");
}

Key TocHandler::databaseKey() {
    openForRead();
    TocHandlerCloser close(*this);

    TocRecord r;

    while ( readNext(r) ) {
        if (r.header_.tag_ == TocRecord::TOC_INIT) {
            eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
            dbUID_ = r.header_.uid_;
            return Key(s);
        }
    }

    throw eckit::SeriousBug("Cannot find a TOC_INIT record");
}

size_t TocHandler::numberOfRecords() const {

    if (count_ == 0) {
        openForRead();
        TocHandlerCloser close(*this);

        TocRecord r;

        while ( readNext(r) ) {
            count_++;
        }
    }

    return count_;
}

const eckit::PathName& TocHandler::directory() const
{
    return directory_;
}

std::vector<Index> TocHandler::loadIndexes() const {

    std::vector<Index> indexes;

    if (!tocPath_.exists()) {
        return indexes;
    }

    openForRead();
    TocHandlerCloser close(*this);

    TocRecord r;
    count_ = 0;


    while ( readNext(r) ) {

        eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
        std::string path;
        std::string type;

        off_t offset;
        std::vector<Index>::iterator j;

        count_++;


        switch (r.header_.tag_) {

        case TocRecord::TOC_INIT:
            dbUID_ = r.header_.uid_;
            // eckit::Log::info() << "TOC_INIT key is " << Key(s) << std::endl;
            break;

        case TocRecord::TOC_INDEX:
            s >> path;
            s >> offset;
            s >> type;
            // eckit::Log::info() << "TOC_INDEX " << path << " - " << offset << std::endl;
            indexes.push_back( new TocIndex(s, directory_, directory_ / path, offset) );
            break;

        case TocRecord::TOC_CLEAR:
            s >> path;
            s >> offset;
            // eckit::Log::info() << "TOC_CLEAR " << path << " - " << offset << std::endl;
            j = std::find_if(indexes.begin(), indexes.end(), HasPath(directory_ / path, offset));
            if (j != indexes.end()) {
                indexes.erase(j);
            }
            break;

        default:
            throw eckit::SeriousBug("Unknown tag in TocRecord", Here());
            break;

        }

    }

    std::reverse(indexes.begin(), indexes.end()); // the entries of the last index takes precedence

    return indexes;

}

const eckit::PathName &TocHandler::tocPath() const {
    return tocPath_;
}

const eckit::PathName &TocHandler::schemaPath() const {
    return schemaPath_;
}


void TocHandler::dump(std::ostream& out, bool simple, bool walkSubTocs) {

    openForRead();
    TocHandlerCloser close(*this);

    TocRecord r;

    while ( readNext(r, walkSubTocs) ) {

        eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
        std::string path;
        std::string type;

        off_t offset;
        std::vector<Index>::iterator j;

        r.dump(out, simple);

        switch (r.header_.tag_) {

            case TocRecord::TOC_INIT: {
                out << "  Key: " << Key(s);
                if(!simple) { out << std::endl; }
                break;
            }

            case TocRecord::TOC_INDEX: {
                s >> path;
                s >> offset;
                s >> type;
                out << "  Path: " << path << ", offset: " << offset << ", type: " << type;
                if(!simple) { out << std::endl; }
                Index index(new TocIndex(s, directory_, directory_ / path, offset));
                index.dump(out, "  ", simple);
                break;
            }

            case TocRecord::TOC_CLEAR: {
                s >> path;
                s >> offset;
                out << "  Path: " << path << ", offset: " << offset << std::endl;
                break;
            }

            case TocRecord::TOC_SUB_TOC: {
                s >> path;
                out << "  Path: " << path << std::endl;
                break;
            }

            default: {
                out << "   Unknown TOC entry" << std::endl;
                break;
            }
        }
        out << std::endl;
    }
}


std::string TocHandler::dbOwner() {
    return userName(dbUID());
}

DbStats TocHandler::stats() const
{
    TocDbStats* stats = new TocDbStats();

    stats->dbCount_         += 1;
    stats->tocRecordsCount_ += numberOfRecords();
    stats->tocFileSize_     += tocPath().size();
    stats->schemaFileSize_  += schemaPath().size();

    return DbStats(stats);
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

    static bool handleLustreStripe = eckit::Resource<bool>("fdbHandleLustreStripe;$FDB_HANDLE_LUSTRE_STRIPE", false);
    return handleLustreStripe;
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


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

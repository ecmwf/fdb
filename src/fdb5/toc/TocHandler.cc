/*
 * (C) Copyright 1996-2013 ECMWF.
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
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/log/BigNum.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/thread/Mutex.h"
#include "eckit/maths/Functions.h"

#include "fdb5/database/Index.h"
#include "fdb5/config/MasterConfig.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/toc/TocIndex.h"


namespace fdb5 {


//----------------------------------------------------------------------------------------------------------------------

static eckit::Mutex local_mutex;

//----------------------------------------------------------------------------------------------------------------------

class TocHandlerCloser {
    TocHandler &handler_;
  public:
    TocHandlerCloser(TocHandler &handler): handler_(handler) {}
    ~TocHandlerCloser() {
        handler_.close();
    }
};

//----------------------------------------------------------------------------------------------------------------------

TocHandler::TocHandler(const eckit::PathName &directory) :
    directory_(directory),
    tocPath_(directory / "toc"),
    schemaPath_(directory / "schema"),
    dbUID_(-1),
    userUID_(::getuid()),
    fd_(-1),
    count_(0) {
}

TocHandler::~TocHandler() {
    close();
}

bool TocHandler::exists() const {
    return tocPath_.exists();
}

void TocHandler::checkUID() {

    static bool fdbOnlyCreatorCanWrite = eckit::Resource<bool>("fdbOnlyCreatorCanWrite", true);
    if (!fdbOnlyCreatorCanWrite) {
        return;
    }

    static std::vector<std::string> fdbSuperUsers = eckit::Resource<std::vector<std::string> >("fdbSuperUsers", "", true);;

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

void TocHandler::openForRead() {

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

    r.header_.size_ = eckit::maths::roundToMultiple(sizeof(TocRecord::Header) + payloadSize, fdbRoundTocRecords);

    size_t len;
    SYSCALL2( len = ::write(fd_, &r, r.header_.size_), tocPath_ );
    ASSERT( len == r.header_.size_ );

}

bool TocHandler::readNext( TocRecord &r ) {

    int len;

    SYSCALL2( len = ::read(fd_, &r, sizeof(TocRecord::Header)), tocPath_ );
    if (len == 0) {
        return false;
    }

    ASSERT(len == sizeof(TocRecord::Header));

    SYSCALL2( len = ::read(fd_, &r.payload_, r.header_.size_ - sizeof(TocRecord::Header)), tocPath_ );
    ASSERT(len == r.header_.size_ - sizeof(TocRecord::Header));

    if ( TocRecord::currentVersion() != r.header_.version_ ) {
        std::ostringstream oss;
        oss << "Record version mistach, expected " << TocRecord::currentVersion()
            << ", got " << r.header_.version_;
        throw eckit::SeriousBug(oss.str());
    }

    return true;
}

void TocHandler::close() {
    if ( fd_ >= 0 ) {
        // eckit::Log::info() << "Closing TOC " << tocPath_ << std::endl;
        SYSCALL2( ::close(fd_), tocPath_ );
        fd_ = -1;
    }
}

void TocHandler::writeInitRecord(const Key &key) {

    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    if ( !directory_.exists() ) {
        directory_.mkdir();
    }

    int iomode = O_CREAT | O_RDWR;
    SYSCALL2(fd_ = ::open( tocPath_.localPath(), iomode, mode_t(0777) ), tocPath_);

    TocHandlerCloser closer(*this);

    TocRecord r;

    size_t len = readNext(r);
    if (len == 0) {

        /* Copy rules first */


        eckit::Log::info() << "Copy schema from "
                           << MasterConfig::instance().schemaPath()
                           << " to "
                           << schemaPath_
                           << std::endl;

        eckit::PathName tmp = eckit::PathName::unique(schemaPath_);

        eckit::FileHandle in(MasterConfig::instance().schemaPath());
        eckit::FileHandle out(tmp);
        in.saveInto(out);

        eckit::PathName::rename(tmp, schemaPath_);

        TocRecord r( TocRecord::TOC_INIT );
        eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
        s << key;
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

    TocRecord r( TocRecord::TOC_CLEAR );
    eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
    s << index.path().baseName();
    s << index.offset();
    append(r, s.position());

    eckit::Log::info() << "TOC_CLEAR " << index.path().baseName() << " - " << index.offset() << std::endl;

}

void TocHandler::writeIndexRecord(const Index &index) {
    openForAppend();
    TocHandlerCloser closer(*this);

    TocRecord r( TocRecord::TOC_INDEX );
    eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
    s << index.path().baseName();
    s << index.offset();
    s << index.type();

    index.encode(s);
    append(r, s.position());

    eckit::Log::info() << "TOC_INDEX " << index.path().baseName() << " - " << index.offset() << " " << index.type() << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------

class HasPath {
    eckit::PathName path_;
    off_t offset_;
  public:
    HasPath(const eckit::PathName &path, off_t offset): path_(path), offset_(offset) {}
    bool operator()(const Index *index) const {
        return (index->path() == path_) && (index->offset() == offset_);
    }
};

long TocHandler::dbUID() {

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

size_t TocHandler::numberOfRecords() {

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

std::vector<Index *> TocHandler::loadIndexes() {

    std::vector<Index *> indexes;

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
        std::vector<Index *>::iterator j;

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
            j = std::find_if (indexes.begin(), indexes.end(), HasPath(directory_ / path, offset));
            if (j != indexes.end()) {
                delete (*j);
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

void TocHandler::freeIndexes(std::vector<Index *> &indexes) {
    for (std::vector<Index *>::iterator j = indexes.begin(); j != indexes.end(); ++j) {
        delete (*j);
    }
    indexes.clear();
}

const eckit::PathName &TocHandler::tocPath() const {
    return tocPath_;
}

const eckit::PathName &TocHandler::schemaPath() const {
    return schemaPath_;
}


void TocHandler::dump(std::ostream& out, bool simple) {

    openForRead();
    TocHandlerCloser close(*this);

    TocRecord r;

    while ( readNext(r) ) {

        eckit::MemoryStream s(&r.payload_[0], r.maxPayloadSize);
        std::string path;
        std::string type;

        off_t offset;
        std::vector<Index *>::iterator j;
        Index *index = 0;

        r.dump(out, simple);

        switch (r.header_.tag_) {

        case TocRecord::TOC_INIT:
            out << "  Key: " << Key(s);
            if(!simple) { out << std::endl; }
            break;

        case TocRecord::TOC_INDEX:
            s >> path;
            s >> offset;
            s >> type;
            out << "  Path: " << path << ", offset: " << offset << ", type: " << type;
            if(!simple) { out << std::endl; }
            index = new TocIndex(s, directory_, directory_ / path, offset);
            index->dump(out, "  ", simple);
            delete index;
            break;

        case TocRecord::TOC_CLEAR:
            s >> path;
            s >> offset;
            out << "  Path: " << path << ", offset: " << offset << std::endl;
            break;

        default:
            out << "   Unknown TOC entry" << std::endl;
            break;

        }
        out << std::endl;

    }


}

std::string TocHandler::dbOwner() {
    return userName(dbUID());
}

std::string TocHandler::userName(long id) const {
  struct passwd *p = getpwuid(id);

  if (p) {
    return p->pw_name;
  } else {
    return eckit::Translator<long, std::string>()(id);
  }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

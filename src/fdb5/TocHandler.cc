/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "eckit/types/DateTime.h"
#include "eckit/config/Resource.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/TocHandler.h"
#include "fdb5/Key.h"
#include "fdb5/Index.h"
#include "fdb5/MasterConfig.h"
#include "fdb5/BTreeIndex.h"

#include "eckit/io/FileHandle.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocHandler::TocHandler(const eckit::PathName &directory) :
    directory_(directory),
    filePath_(directory / "toc"),
    fd_(-1)
{
}

TocHandler::~TocHandler() {
    close();
}

bool TocHandler::exists() const {
    return filePath_.exists();
}

void TocHandler::openForAppend() {

    eckit::Log::info() << "Opening for append TOC " << filePath_ << std::endl;

    int iomode = O_WRONLY | O_APPEND;
    //#ifdef __linux__
    //  iomode |= O_NOATIME;
    //#endif
    SYSCALL2((fd_ = ::open( filePath_.asString().c_str(), iomode, (mode_t)0777 )), filePath_);
}

void TocHandler::openForRead() {

    eckit::Log::info() << "Opening for read TOC " << filePath_ << std::endl;

    int iomode = O_RDONLY;
    //#ifdef __linux__
    //  iomode |= O_NOATIME;
    //#endif
    SYSCALL2((fd_ = ::open( filePath_.asString().c_str(), iomode )), filePath_ );
}

void TocHandler::append( const TocRecord &r ) {

    try {
        size_t len;
        SYSCALL2( len = ::write(fd_, &r, sizeof(TocRecord)), filePath_ );
        ASSERT( len == sizeof(TocRecord) );
    } catch (...) {
        close();
        throw;
    }
}

size_t TocHandler::readNext( TocRecord &r ) {

    int len;

    SYSCALL2( len = ::read(fd_, &r, sizeof(TocRecord)), filePath_ );
    if (len == 0) {
        return len;
    }

    if (len != sizeof(TocRecord)) {
        close();
        std::ostringstream msg;
        msg << "Failed to read complete TocRecord from " << filePath_.asString();
        throw eckit::ReadError( msg.str() );
    }

    if ( TocRecord::currentTagVersion() != r.version() ) {
        std::ostringstream oss;
        oss << "Record version mistach, expected " << int(TocRecord::currentTagVersion())
            << ", got " << int(r.version());
        throw eckit::SeriousBug(oss.str());
    }

    ASSERT( r.isComplete() );

    return len;
}

void TocHandler::close() {
    if ( fd_ >= 0 ) {
        eckit::Log::info() << "Closing TOC " << filePath_ << std::endl;
        SYSCALL2( ::close(fd_), filePath_ );
        fd_ = -1;
    }
}

void TocHandler::printRecord(const TocRecord &r, std::ostream &os) {
    switch (r.head_.tag_) {
    case TOC_INIT:
        os << "Record TocInit [" << eckit::DateTime( r.head_.timestamp_.tv_sec ) << "]";
        break;

    case TOC_INDEX:
        os << "Record IdxFinal [" << eckit::DateTime( r.head_.timestamp_.tv_sec ) << "]";
        break;

    case TOC_CLEAR:
        os << "Record IdxCancel [" << eckit::DateTime( r.head_.timestamp_.tv_sec ) << "]";
        break;

    case TOC_WIPE:
        os << "Record TocWipe [" << eckit::DateTime( r.head_.timestamp_.tv_sec ) << "]";
        break;

    default:
        throw eckit::SeriousBug("Unknown tag in TocRecord", Here());
        break;
    }
}

void TocHandler::writeInitRecord(const Key &key) {

    if ( !directory_.exists() ) {
        directory_.mkdir();
    }

    int iomode = O_CREAT | O_RDWR;
    fd_ = ::open( filePath_.asString().c_str(), iomode, mode_t(0777) );

    TocRecord r;

    size_t len = readNext(r);
    if (len == 0) {

        /* Copy rules first */

        eckit::PathName schemaPath(directory_ / "schema");

        eckit::Log::info() << "Copy schema from "
                           << MasterConfig::instance().schemaPath()
                           << " to "
                           << schemaPath
                           << std::endl;

        eckit::PathName tmp = eckit::PathName::unique(schemaPath);

        eckit::FileHandle in(MasterConfig::instance().schemaPath());
        eckit::FileHandle out(tmp);
        in.saveInto(out);

        eckit::PathName::rename(tmp, schemaPath);

        TocRecord r( TOC_INIT );
        eckit::MemoryStream s(r.payload_.data(), r.payload_size);
        s << key;

        append(r);
    } else {
        ASSERT(r.head_.tag_ == TOC_INIT);
    }

    close();
}

void TocHandler::writeClearRecord(const eckit::PathName &path) {
    openForAppend();

    TocRecord r( TOC_CLEAR );

    eckit::MemoryStream s(r.payload_.data(), r.payload_size);

    s << path.baseName();

    append(r);
    close();

}

void TocHandler::writeWipeRecord() {
    openForAppend();

    TocRecord r( TOC_WIPE );

    append(r);
    close();

}


void TocHandler::writeIndexRecord(const Index &index) {
    openForAppend();

    TocRecord r( TOC_INDEX );

    eckit::MemoryStream s(r.payload_.data(), r.payload_size);

    s << index.path().baseName();
    index.encode(s);

    append(r);
    close();

}

//==========================================================================

class HasPath {
    eckit::PathName path_;
public:
    HasPath(const eckit::PathName& path): path_(path) {}
    bool operator()(const Index* index) const {
        return index->path() == path_;
    }
};

std::vector<Index*> TocHandler::loadIndexes() {

    openForRead();

    TocRecord r;

    std::vector<Index *> indexes;

    while ( readNext(r) ) {

        eckit::Log::info() << "TocRecord " << r << std::endl;

        eckit::MemoryStream s(r.payload_.data(), r.payload_size);
        std::string path;
        std::vector<Index *>::iterator j;


        switch (r.head_.tag_) {

        case TOC_INIT:
            eckit::Log::info() << "TOC_INIT key is " << Key(s) << std::endl;
            break;

        case TOC_INDEX:
            s >> path;
            indexes.push_back( new BTreeIndex(s, directory_ / path) );
            break;

        case TOC_CLEAR:
            s >> path;
            j = std::find_if (indexes.begin(), indexes.end(), HasPath(directory_ / path));
            if(j != indexes.end()) {
                delete (*j);
                indexes.erase(j);
            }
            break;

        case TOC_WIPE:
            freeIndexes(indexes);
            break;

        default:
            throw eckit::SeriousBug("Unknown tag in TocRecord", Here());
            break;

        }

        std::reverse(indexes.begin(), indexes.end()); // the entries of the last index takes precedence
    }

    close();

    return indexes;

}

void TocHandler::freeIndexes(std::vector<Index *>& indexes) {
    for(std::vector<Index *>::iterator j = indexes.begin(); j != indexes.end(); ++j) {
        delete (*j);
    }
    indexes.clear();
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

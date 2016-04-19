/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "mars_server_config.h"

#include <cstring>
#include <fcntl.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>

#ifdef EC_HAVE_SYS_VFS_H
#include <sys/vfs.h>
#endif

#ifdef EC_HAVE_SYS_MOUNT_H
#include <sys/param.h>
#include <sys/mount.h>
#endif

#include "eckit/exception/Exceptions.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/config/Resource.h"
#include "eckit/log/Bytes.h"
#include "eckit/log/Plural.h"
#include "eckit/log/Timer.h"
#include "eckit/io/AIOHandle.h"

#include "fdb5/Error.h"
#include "fdb5/MasterConfig.h"
#include "fdb5/TocActions.h"
#include "fdb5/TocDBWriter.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocDBWriter::TocDBWriter(const Key& key) :
    TocDB(key),
    current_(0),
    dirty_(false)
{
    if( !path_.exists() )
    {
        TocInitialiser init(path_);
    }

    blockSize_ = eckit::Resource<long>( "blockSize", 0 );

    if( blockSize_ < 0 ) // take blockSize_ from statfs
    {
        struct statfs s;
        SYSCALL2( statfs(const_cast<char*>( path_.localPath() ), &s ), path_ );
        if( s.f_bsize > 0 )
            blockSize_ = s.f_bsize;
        /// @note should we use s.f_iosize, optimal transfer block size, on macosx ?
    }

    if( blockSize_ > 0 ) {
        padding_.resize(blockSize_,'\0');
    }

    aio_ = eckit::Resource<bool>("fdbAsyncWrite",false);

    Log::info() << "TocDBWriter for TOC [" << path_ << "] with block size of " << Bytes(blockSize_) << std::endl;
}

TocDBWriter::~TocDBWriter()
{
    close();
}

bool TocDBWriter::selectIndex(const Key& key)
{
    TocIndex& toc = getTocIndex(key);
    current_ = &getIndex(key, toc.index());
    return true;
}

bool TocDBWriter::open() {
    return true;
}

void TocDBWriter::close() {

    Log::info() << "Closing path " << path_ << std::endl;

    flush(); // closes the TOC entries & indexes

    closeDataHandles(); // close data handles
}



void TocDBWriter::index(const Key& key, const PathName& path, Offset offset, Length length)
{
    ASSERT(current_);

    Index::Field field(path, offset, length);

    Log::debug(2) << " indexing {" << key << "," << field << "}" << std::endl;

    current_->put(key, field);
}

void TocDBWriter::archive(const Key& key, const void *data, Length length)
{
    ASSERT(current_);
    dirty_ = true;

    PathName dataPath = getDataPath(current_->key());

    eckit::DataHandle& dh = getDataHandle(dataPath);

    Offset position = dh.position();

    dh.write( data, length );

    if( blockSize_ > 0 ) // padding?
    {
        long long len = length;
        size_t paddingSize       =  (( len + blockSize_-1 ) / blockSize_ ) * blockSize_ - len;
        if(paddingSize)
            dh.write( &padding_[0], paddingSize );
    }

    Index::Field field (dataPath, position, length);

    Log::debug(2) << " pushing {" << key << "," << field << "}" << std::endl;

    current_->put(key, field);
}

void TocDBWriter::flush()
{
    if(!dirty_) {
        return;
    }

    dirty_ = false;
    // ensure consistent state before writing Toc entry

    flushDataHandles();
    flushIndexes();

    // close the indexes before the Toc's
    closeIndexes();

    // finally write all Toc entries
    closeTocEntries();
}

Index* TocDBWriter::openIndex(const Key& key, const PathName& path) const
{
    return Index::create(key, indexType_, path, Index::WRITE );
}

eckit::DataHandle* TocDBWriter::getCachedHandle( const PathName& path ) const
{
    HandleStore::const_iterator itr = handles_.find( path );
    if( itr != handles_.end() )
        return itr->second;
    else
        return 0;
}

void TocDBWriter::closeDataHandles()
{
    for( HandleStore::iterator itr = handles_.begin(); itr != handles_.end(); ++itr )
    {
        eckit::DataHandle* dh = itr->second;
        if( dh )
        {
            dh->close();
            delete dh;
        }
    }
    handles_.clear();
}

eckit::DataHandle* TocDBWriter::createFileHandle(const PathName& path)
{
    return path.fileHandle();
}

DataHandle* TocDBWriter::createAsyncHandle(const PathName& path)
{
    size_t nbBuffers  = eckit::Resource<unsigned long>("fdbNbAsyncBuffers",4);
    size_t sizeBuffer = eckit::Resource<unsigned long>("fdbSizeAsyncBuffer",64*1024*1024);

    return new AIOHandle(path,nbBuffers,sizeBuffer);
}

eckit::DataHandle* TocDBWriter::createPartHandle(const PathName& path, Offset offset, Length length)
{
    return path.partHandle(offset,length);
}

DataHandle& TocDBWriter::getDataHandle( const PathName& path )
{
    eckit::DataHandle* dh = getCachedHandle( path );
    if( !dh )
    {
        dh = aio_ ? createAsyncHandle( path ) : createFileHandle( path );
        handles_[path] = dh;
        ASSERT( dh );
        dh->openForAppend(0);
    }
    return *dh;
}

TocIndex& TocDBWriter::getTocIndex(const Key& key)
{
    TocIndex* toc = 0;

    TocIndexStore::const_iterator itr = tocEntries_.find( key );
    if( itr != tocEntries_.end() )
    {
        toc = itr->second;
    }
    else
    {
        toc = new TocIndex(path_, generateIndexPath(key), key);
        tocEntries_[ key ] = toc;
    }

    ASSERT( toc );

    return *toc;
}

eckit::PathName TocDBWriter::generateIndexPath(const Key& key) const
{
    PathName tocPath ( path_ );
    tocPath /= key.valuesToString();
    tocPath = PathName::unique(tocPath) + ".idx";
    return tocPath;
}

eckit::PathName TocDBWriter::generateDataPath(const Key& key) const
{
    PathName dpath ( path_ );
    dpath /=  key.valuesToString();
    dpath = PathName::unique(dpath) + ".dat";
    return dpath;
}

PathName TocDBWriter::getDataPath(const Key& key)
{
    PathStore::const_iterator itr = dataPaths_.find(key);
    if( itr != dataPaths_.end() )
        return itr->second;

    PathName dataPath = generateDataPath(key);

    dataPaths_[ key ] = dataPath;

    return dataPath;
}

void TocDBWriter::flushIndexes()
{
    Timer timer("TocDBWriter::flushIndexes()");
    for(IndexStore::iterator itr = indexes_.begin(); itr != indexes_.end(); ++itr )
    {
        Index* idx = itr->second;
        if( idx )
            idx->flush();
    }
}

void TocDBWriter::flushDataHandles()
{
    Timer timer("TocDBWriter::flushDataHandles()");

    Log::info() << "Flushing " << Plural(handles_.size(),"data handle") << std::endl;

    for(HandleStore::iterator itr = handles_.begin(); itr != handles_.end(); ++itr)
    {
        eckit::DataHandle* dh = itr->second;
        if( dh ) {
            std::ostringstream oss;
            oss << *dh;
            Timer timer2(oss.str());
            dh->flush();
        }
    }
}

void TocDBWriter::closeTocEntries()
{
    Timer timer("TocDBWriter::closeTocEntries()");
    for( TocIndexStore::iterator itr = tocEntries_.begin(); itr != tocEntries_.end(); ++itr )
    {
        delete itr->second;
    }
    tocEntries_.clear();
}

void TocDBWriter::print(std::ostream &out) const
{
    out << "TocDBWriter("
        /// @todo should print more here
        << ")";
}

DBBuilder<TocDBWriter> TocDBWriter_Builder("toc.writer");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

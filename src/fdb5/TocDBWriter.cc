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
#include "eckit/io/AIOHandle.h"

#include "fdb5/Error.h"
#include "fdb5/MasterConfig.h"
#include "fdb5/TocActions.h"
#include "fdb5/TocDBWriter.h"
#include "fdb5/TocSchema.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocDBWriter::TocDBWriter(const Key& key) :
    TocDB(key),
    current_(0)
{
    if( !path_.exists() )
    {
        TocInitialiser init(path_);
    }

    blockSize_ = eckit::Resource<long>( "blockSize", -1 );

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
    current_ = &getIndex( toc.index() );
}

bool TocDBWriter::open() {
    return true;
}

void TocDBWriter::close() {

    // ensure consistent state before writing Toc entry

    flush();

    // close all data handles followed by all indexes, before writing Toc entry

    closeDataHandles();

    // finally write all Toc entries

    closeTocEntries();
}


void TocDBWriter::archive(const Key& key, const void *data, Length length)
{
    ASSERT(current_);

    PathName dataPath = getDataPath(key);

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
    flushIndexes();
    flushDataHandles();
}

Index* TocDBWriter::openIndex(const PathName& path) const
{
    return Index::create( indexType_, path, Index::WRITE );
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

    std::string tocEntry = key.valuesToString();

    TocIndexStore::const_iterator itr = tocEntries_.find( tocEntry );
    if( itr != tocEntries_.end() )
    {
        toc = itr->second;
    }
    else
    {
        toc = new TocIndex(path_, generateIndexPath(key), key.valuesToString());
        tocEntries_[ tocEntry ] = toc;
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
    IndexStore::iterator itr = indexes_.begin();
    for( ; itr != indexes_.end(); ++itr )
    {
        Index* idx = itr->second;
        if( idx )
            idx->flush();
    }
}

void TocDBWriter::flushDataHandles()
{
    for(HandleStore::iterator itr = handles_.begin(); itr != handles_.end(); ++itr)
    {
        eckit::DataHandle* dh = itr->second;
        if( dh )
            dh->flush();
    }
}

void TocDBWriter::closeTocEntries()
{
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

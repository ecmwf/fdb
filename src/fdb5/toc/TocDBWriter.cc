/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "mars_server_config.h"

#include "eckit/config/Resource.h"
#include "eckit/log/Log.h"
#include "eckit/log/Bytes.h"
#include "eckit/io/AIOHandle.h"

#include "fdb5/LibFdb.h"

#include "fdb5/io/FDBFileHandle.h"

#include "fdb5/toc/TocDBWriter.h"
#include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/TocFieldLocation.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


TocDBWriter::TocDBWriter(const Key &key, const eckit::Configuration& config) :
    TocDB(key, config),
    dirty_(false) {
    writeInitRecord(key);
    loadSchema();
    checkUID();
}

TocDBWriter::TocDBWriter(const eckit::PathName &directory, const eckit::Configuration& config) :
    TocDB(directory, config),
    dirty_(false) {

    NOTIMP; // TODO: Not clear what should occur here for writeInitRecord.

    loadSchema();
    checkUID();
}

TocDBWriter::~TocDBWriter() {
    close();
}

bool TocDBWriter::selectIndex(const Key& key) {
    currentIndexKey_ = key;

    if (indexes_.find(key) == indexes_.end()) {
        PathName indexPath(generateIndexPath(key));

        // Enforce lustre striping if requested
        if (stripeLustre()) {
            LustreStripe stripe = stripeIndexLustreSettings();
            fdb5LustreapiFileCreate(indexPath.localPath(), stripe.size_, stripe.count_);
        }

        indexes_[key] = Index(new TocIndex(key, indexPath, 0, TocIndex::WRITE));
    }

    current_ = indexes_[key];
    current_.open();

    // If we are using subtocs, then we need to maintain a duplicate index that doesn't get flushed
    // each step.

    if (useSubToc()) {

        if (fullIndexes_.find(key) == fullIndexes_.end()) {

            // TODO TODO TODO .master.index
            PathName indexPath(generateIndexPath(key));

            // Enforce lustre striping if requested
            if (stripeLustre()) {
                LustreStripe stripe = stripeIndexLustreSettings();
                fdb5LustreapiFileCreate(indexPath.localPath(), stripe.size_, stripe.count_);
            }

            fullIndexes_[key] = Index(new TocIndex(key, indexPath, 0, TocIndex::WRITE));
        }

        currentFull_ = fullIndexes_[key];
        currentFull_.open();
    }

    return true;
}

void TocDBWriter::deselectIndex() {
    current_ = Index();
    currentFull_ = Index();
    currentIndexKey_ = Key();
}

bool TocDBWriter::open() {
    return true;
}

void TocDBWriter::close() {

    eckit::Log::debug<LibFdb>() << "Closing path " << directory_ << std::endl;

    flush(); // closes the TOC entries & indexes but not data files

    compactSubTocIndexes();

    deselectIndex();

    closeDataHandles();

    closeIndexes();
}

void TocDBWriter::index(const Key &key, const eckit::PathName &path, eckit::Offset offset, eckit::Length length) {
    dirty_ = true;

    if (current_.null()) {
        ASSERT(!currentIndexKey_.empty());
        selectIndex(currentIndexKey_);
    }

    Field field(TocFieldLocation(path, offset, length));

    current_.put(key, field);

    if (useSubToc())
        currentFull_.put(key, field);
}

void TocDBWriter::archive(const Key &key, const void *data, eckit::Length length) {
    dirty_ = true;

    if (current_.null()) {
        ASSERT(!currentIndexKey_.empty());
        selectIndex(currentIndexKey_);
    }

    eckit::PathName dataPath = getDataPath(current_.key());

    eckit::DataHandle &dh = getDataHandle(dataPath);

    eckit::Offset position = dh.position();

    dh.write( data, length );

    Field field (TocFieldLocation(dataPath, position, length));

    current_.put(key, field);

    if (useSubToc())
        currentFull_.put(key, field);
}

void TocDBWriter::flush() {
    if (!dirty_) {
        return;
    }

    // ensure consistent state before writing Toc entry

    flushDataHandles();
    flushIndexes();

    dirty_ = false;
    current_ = Index();
    currentFull_ = Index();
}


eckit::DataHandle *TocDBWriter::getCachedHandle( const eckit::PathName &path ) const {
    HandleStore::const_iterator j = handles_.find( path );
    if ( j != handles_.end() )
        return j->second;
    else
        return 0;
}

void TocDBWriter::closeDataHandles() {
    for ( HandleStore::iterator j = handles_.begin(); j != handles_.end(); ++j ) {
        eckit::DataHandle *dh = j->second;
        dh->close();
        delete dh;
    }
    handles_.clear();
}


eckit::DataHandle *TocDBWriter::createFileHandle(const eckit::PathName &path) {

    static size_t sizeBuffer = eckit::Resource<unsigned long>("fdbBufferSize", 64 * 1024 * 1024);

    if(stripeLustre()) {

        eckit::Log::debug<LibFdb>() << "Creating LustreFileHandle<FDBFileHandle> to " << path
                                    << " buffer size " << sizeBuffer
                                    << std::endl;

        return new LustreFileHandle<FDBFileHandle>(path, sizeBuffer, stripeDataLustreSettings());
    }

    eckit::Log::debug<LibFdb>() << "Creating FDBFileHandle to " << path
                                << " with buffer of " << eckit::Bytes(sizeBuffer)
                                << std::endl;

    return new FDBFileHandle(path, sizeBuffer);
}

eckit::DataHandle *TocDBWriter::createAsyncHandle(const eckit::PathName &path) {

    static size_t nbBuffers  = eckit::Resource<unsigned long>("fdbNbAsyncBuffers", 4);
    static size_t sizeBuffer = eckit::Resource<unsigned long>("fdbSizeAsyncBuffer", 64 * 1024 * 1024);

    if(stripeLustre()) {

        eckit::Log::debug<LibFdb>() << "Creating LustreFileHandle<AIOHandle> to " << path
                                    << " with " << nbBuffers
                                    << " buffer each with " << eckit::Bytes(sizeBuffer)
                                    << std::endl;

        return new LustreFileHandle<eckit::AIOHandle>(path, nbBuffers, sizeBuffer, stripeDataLustreSettings());
    }

    return new eckit::AIOHandle(path, nbBuffers, sizeBuffer);
}


eckit::DataHandle &TocDBWriter::getDataHandle( const eckit::PathName &path ) {
    eckit::DataHandle *dh = getCachedHandle( path );
    if ( !dh ) {
        static bool fdbAsyncWrite = eckit::Resource<bool>("fdbAsyncWrite;$FDB_ASYNC_WRITE", false);

        dh = fdbAsyncWrite ? createAsyncHandle( path ) : createFileHandle( path );
        handles_[path] = dh;
        ASSERT( dh );
        dh->openForAppend(0);
    }
    return *dh;
}

eckit::PathName TocDBWriter::generateIndexPath(const Key &key) const {
    eckit::PathName tocPath ( directory_ );
    tocPath /= key.valuesToString();
    tocPath = eckit::PathName::unique(tocPath) + ".index";
    return tocPath;
}

eckit::PathName TocDBWriter::generateDataPath(const Key &key) const {
    eckit::PathName dpath ( directory_ );
    dpath /=  key.valuesToString();
    dpath = eckit::PathName::unique(dpath) + ".data";
    return dpath;
}

eckit::PathName TocDBWriter::getDataPath(const Key &key) {
    PathStore::const_iterator j = dataPaths_.find(key);
    if ( j != dataPaths_.end() )
        return j->second;

    eckit::PathName dataPath = generateDataPath(key);

    dataPaths_[ key ] = dataPath;

    return dataPath;
}

// n.b. We do _not_ flush the fullIndexes_ set of indexes.
// The indexes pointed to in the indexes_ list get written out each time there is
// a flush (i.e. every step). The indexes stored in fullIndexes then contain _all_
// the data that is indexes thorughout the lifetime of the DBWriter, which can be
// compacted later for read performance.
void TocDBWriter::flushIndexes() {
    for (IndexStore::iterator j = indexes_.begin(); j != indexes_.end(); ++j ) {
        Index& idx = j->second;

        if (idx.dirty()) {
            idx.flush();
            writeIndexRecord(idx);
            idx.reopen(); // Create a new btree
        }
    }
}


void TocDBWriter::closeIndexes() {
    for (IndexStore::iterator j = indexes_.begin(); j != indexes_.end(); ++j ) {
        Index& idx = j->second;
        idx.close();
    }

    for (IndexStore::iterator j = fullIndexes_.begin(); j != fullIndexes_.end(); ++j ) {
        Index& idx = j->second;
        idx.close();
    }

    indexes_.clear(); // all indexes instances destroyed
    fullIndexes_.clear(); // all indexes instances destroyed
}

void TocDBWriter::flushDataHandles() {

    for (HandleStore::iterator j = handles_.begin(); j != handles_.end(); ++j) {
        eckit::DataHandle *dh = j->second;
        dh->flush();
    }
}

void TocDBWriter::compactSubTocIndexes() {

    // In this routine, we write out indexes that correspond to all of the data in the
    // subtoc, written by this process. Then we append a masking entry.

    Buffer buf(sizeof(TocRecord) * (fullIndexes_.size() + 1));
    size_t combinedSize = 0;

    if (useSubToc()) {
        Log::info() << "compacting sub tocs" << std::endl;

        for (IndexStore::iterator j = fullIndexes_.begin(); j != fullIndexes_.end(); ++j) {
            Index& idx = j->second;

            if (idx.dirty()) {

                idx.flush();
                TocRecord* r = new (&buf[combinedSize]) TocRecord(TocRecord::TOC_INDEX);
                combinedSize += roundRecord(*r, buildIndexRecord(*r, idx));
            }
        }

        // And add the masking record for the subtoc

        TocRecord* r = new (&buf[combinedSize]) TocRecord(TocRecord::TOC_MASK_SUB_TOC);
        combinedSize += roundRecord(*r, buildSubTocMaskRecord(*r));

        // Write all of these  records to the toc in one go.

        appendBlock(buf, combinedSize);
    }
}


void TocDBWriter::print(std::ostream &out) const {
    out << "TocDBWriter(" << directory() << ")";
}

static DBBuilder<TocDBWriter> builder("toc.writer", false, true);

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

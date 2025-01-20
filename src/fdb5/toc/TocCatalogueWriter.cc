/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/log/Log.h"
#include "eckit/types/Types.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/io/LustreSettings.h"
#include "fdb5/toc/RootManager.h"
#include "fdb5/toc/TocCatalogue.h"
#include "fdb5/toc/TocCatalogueWriter.h"
#include "fdb5/toc/TocFieldLocation.h"
#include "fdb5/toc/TocIndex.h"
#include <sstream>

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


TocCatalogueWriter::TocCatalogueWriter(const Key& dbKey, const fdb5::Config& config) :
    TocCatalogue(dbKey, config),
    umask_(config.umask()),
    archivedLocations_(0) {
    writeInitRecord(dbKey);
    TocCatalogue::loadSchema();
    TocCatalogue::checkUID();
}

TocCatalogueWriter::TocCatalogueWriter(const eckit::URI &uri, const fdb5::Config& config) :
    TocCatalogue(uri.path(), ControlIdentifiers{}, config),
    umask_(config.umask()),
    archivedLocations_(0) {
    writeInitRecord(TocCatalogue::key());
    TocCatalogue::loadSchema();
    TocCatalogue::checkUID();
}

TocCatalogueWriter::~TocCatalogueWriter() {
    clean();
    close();
}

// selectIndex is called during schema traversal and in case of out-of-order fieldLocation archival
bool TocCatalogueWriter::selectIndex(const Key& idxKey) {
    currentIndexKey_ = idxKey;

    if (indexes_.find(idxKey) == indexes_.end()) {
        PathName indexPath(generateIndexPath(idxKey));

        // Enforce lustre striping if requested
        if (stripeLustre()) {
            fdb5LustreapiFileCreate(indexPath, stripeIndexLustreSettings());
        }

        indexes_[idxKey] = Index(new TocIndex(idxKey, *this, indexPath, 0, TocIndex::WRITE));
    }

    current_ = indexes_[idxKey];
    current_.open();
    current_.flock();

    // If we are using subtocs, then we need to maintain a duplicate index that doesn't get flushed
    // each step.

    if (useSubToc()) {

        if (fullIndexes_.find(idxKey) == fullIndexes_.end()) {

            // TODO TODO TODO .master.index
            PathName indexPath(generateIndexPath(idxKey));

            // Enforce lustre striping if requested
            if (stripeLustre()) {
                fdb5LustreapiFileCreate(indexPath, stripeIndexLustreSettings());
            }

            fullIndexes_[idxKey] = Index(new TocIndex(idxKey, *this, indexPath, 0, TocIndex::WRITE));
        }

        currentFull_ = fullIndexes_[idxKey];
        currentFull_.open();
        currentFull_.flock();
    }

    return true;
}

void TocCatalogueWriter::deselectIndex() {
    current_ = Index();
    currentFull_ = Index();
    currentIndexKey_ = Key();
}

bool TocCatalogueWriter::open() {
    return true;
}

void TocCatalogueWriter::clean() {

    LOG_DEBUG_LIB(LibFdb5) << "Closing path " << directory_ << std::endl;

    flush(archivedLocations_); // closes the TOC entries & indexes but not data files

    compactSubTocIndexes();

    deselectIndex();
}

void TocCatalogueWriter::close() {

    closeIndexes();
}

void TocCatalogueWriter::index(const Key& key, const eckit::URI &uri, eckit::Offset offset, eckit::Length length) {
    archivedLocations_++;

    if (current_.null()) {
        ASSERT(!currentIndexKey_.empty());
        selectIndex(currentIndexKey_);
    }

    Field field(TocFieldLocation(uri, offset, length, Key()), currentIndex().timestamp());

    current_.put(key, field);

    if (useSubToc())
        currentFull_.put(key, field);
}

void TocCatalogueWriter::reconsolidateIndexesAndTocs() {

    // TODO: This tool needs to be rewritten to reindex properly using the schema.
    //       Currently it results in incomplete indexes if data has been written
    //       in a context that has optional values in the schema.

    // Visitor class for reindexing

    class ConsolidateIndexVisitor : public EntryVisitor {
    public:
        ConsolidateIndexVisitor(TocCatalogueWriter& writer) :
            writer_(writer) {}
        ~ConsolidateIndexVisitor() override {}
    private:
        void visitDatum(const Field& field, const Key& datumKey) override {
            // TODO: Do a sneaky schema.expand() here, prepopulated with the current DB/index/Rule,
            //       to extract the full key, including optional values.
            const TocFieldLocation& location(static_cast<const TocFieldLocation&>(field.location()));
            writer_.index(datumKey, location.uri(), location.offset(), location.length());

        }
        void visitDatum(const Field& field, const std::string& keyFingerprint) override {
            EntryVisitor::visitDatum(field, keyFingerprint);
        }

        TocCatalogueWriter& writer_;
    };

    // Visit all tocs and indexes

    std::set<std::string> subtocs;
    std::vector<bool> indexInSubtoc;
    std::vector<Index> readIndexes = loadIndexes(false, &subtocs, &indexInSubtoc);
    size_t maskable_indexes = 0;

    ConsolidateIndexVisitor visitor(*this);

    ASSERT(readIndexes.size() == indexInSubtoc.size());

    for (size_t i = 0; i < readIndexes.size(); i++) {
        Index& idx(readIndexes[i]);
        selectIndex(idx.key());
        idx.entries(visitor);

        Log::info() << "Visiting index: " << idx.location().uri() << std::endl;

        // We need to explicitly mask indexes in the master TOC
        if (!indexInSubtoc[i]) maskable_indexes += 1;
    }

    // Flush the new indexes and add relevant entries!
    clean();
    close();

    // Add masking entries for all the indexes and subtocs visited so far

    Buffer buf(sizeof(TocRecord) * (subtocs.size() + maskable_indexes));
    size_t combinedSize = 0;

    for (size_t i = 0; i < readIndexes.size(); i++) {
        // We need to explicitly mask indexes in the master TOC
        if (!indexInSubtoc[i]) {
            Index& idx(readIndexes[i]);
            TocRecord* r = new (&buf[combinedSize]) TocRecord(serialisationVersion().used(), TocRecord::TOC_CLEAR);
            combinedSize += roundRecord(*r, buildClearRecord(*r, idx));
            Log::info() << "Masking index: " << idx.location().uri() << std::endl;
        }
    }

    for (const std::string& subtoc_path : subtocs) {
        TocRecord* r = new (&buf[combinedSize]) TocRecord(serialisationVersion().used(), TocRecord::TOC_CLEAR);
        combinedSize += roundRecord(*r, buildSubTocMaskRecord(*r, subtoc_path));
        Log::info() << "Masking sub-toc: " << subtoc_path << std::endl;
    }

    // And write all the TOC records in one go!

    appendBlock(buf, combinedSize);
}

const Index& TocCatalogueWriter::currentIndex() {

    if (current_.null()) {
        ASSERT(!currentIndexKey_.empty());
        selectIndex(currentIndexKey_);
    }

    return current_;
}

const Key TocCatalogueWriter::currentIndexKey() {
    currentIndex();
    return currentIndexKey_;
}

const TocSerialisationVersion& TocCatalogueWriter::serialisationVersion() const {
    return TocHandler::serialisationVersion();
}

void TocCatalogueWriter::overlayDB(const Catalogue* srcCatalogue, const eckit::StringSet& variableKeys, bool unmount) {

    const auto* source = dynamic_cast<const TocCatalogue*>(srcCatalogue);

    if (!source) { throw eckit::UserError("Cannot overlay toc DB with a non-toc source DB!", Here()); }

    if (!source->exists()) {
        std::ostringstream oss;
        oss << "Source database is not found: " << *source << std::endl;
        throw eckit::UserError(oss.str(), Here());
    }

    if (source->uri() == uri()) {
        std::ostringstream oss;
        oss << "Source and target cannot be same! " << source->uri() << " : " << uri();
        throw eckit::UserError(oss.str(), Here());
    }

    const auto& srcKey = source->key();

    // check keywords are matching
    if (srcKey.keys() != dbKey_.keys()) {
        std::ostringstream oss;
        oss << "Keys are not matching! " << srcKey << " : " << dbKey_ << std::endl;
        throw eckit::UserError(oss.str(), Here());
    }

    // check values are as expected
    for (const auto& [keyword, value] : dbKey_) {

        const auto isDifferent = srcKey.find(keyword)->second != value;
        const auto isVariable  = variableKeys.find(keyword) != variableKeys.end();

        if (isDifferent && !isVariable) {
            std::ostringstream oss;
            oss << "Keyword [" << keyword << "] must not differ between DBs: " << srcKey << " : " << dbKey_;
            throw eckit::UserError(oss.str(), Here());
        }

        if (isVariable && !isDifferent) {
            std::ostringstream oss;
            oss << "Variable Keyword [" << keyword << "] must differ between DBs: " << srcKey << " : " << dbKey_;
            throw eckit::UserError(oss.str(), Here());
        }
    }

    // And append the mount link / unmount mask
    if (unmount) {

        // First sanity check that we are already mounted

        eckit::StringSet subtocs;
        loadIndexes(false, &subtocs);

        eckit::PathName stPath(source->tocPath());
        if (subtocs.find(stPath) == subtocs.end()) {
            std::stringstream ss;
            ss << "Cannot unmount source DB [" << source << "] that is already unmounted!";
            throw eckit::UserError(ss.str(), Here());
        }

        writeSubTocMaskRecord(*source);
    } else {
        writeSubTocRecord(*source);
    }
}

void TocCatalogueWriter::hideContents() {
    writeClearAllRecord();
}

bool TocCatalogueWriter::enabled(const ControlIdentifier& controlIdentifier) const {
    if (controlIdentifier == ControlIdentifier::List || controlIdentifier == ControlIdentifier::Retrieve) {
        return false;
    }
    return TocCatalogue::enabled(controlIdentifier);
}

void TocCatalogueWriter::archive(const Key& idxKey, const Key& datumKey, std::shared_ptr<const FieldLocation> fieldLocation) {
    archivedLocations_++;

    if (current_.null()) {
        ASSERT(!currentIndexKey_.empty());
        selectIndex(currentIndexKey_);
    } else {
        // in case of async archival (out of order store/catalogue archival), currentIndexKey_ can differ from the indexKey used for store archival. Reset it
        if(currentIndexKey_ != idxKey) {
            selectIndex(idxKey);
        }
    }

    Field field(std::move(fieldLocation), currentIndex().timestamp());

    current_.put(datumKey, field);

    if (useSubToc())
        currentFull_.put(datumKey, field);
}

void TocCatalogueWriter::flush(size_t archivedFields) {
    ASSERT(archivedFields == archivedLocations_);

    if (archivedLocations_ == 0) {
        return;
    }

    flushIndexes();

    archivedLocations_ = 0;
    current_ = Index();
    currentFull_ = Index();
}

eckit::PathName TocCatalogueWriter::generateIndexPath(const Key& key) const {
    eckit::PathName tocPath ( directory_ );
    tocPath /= key.valuesToString();
    tocPath = eckit::PathName::unique(tocPath) + ".index";
    return tocPath;
}

// n.b. We do _not_ flush the fullIndexes_ set of indexes.
// The indexes pointed to in the indexes_ list get written out each time there is
// a flush (i.e. every step). The indexes stored in fullIndexes then contain _all_
// the data that is indexes thorughout the lifetime of the DBWriter, which can be
// compacted later for read performance.
void TocCatalogueWriter::flushIndexes() {
    for (IndexStore::iterator j = indexes_.begin(); j != indexes_.end(); ++j ) {
        Index& idx = j->second;

        if (idx.dirty()) {
            idx.flush();
            writeIndexRecord(idx);
            idx.reopen(); // Create a new btree
        }
    }
}


void TocCatalogueWriter::closeIndexes() {
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

void TocCatalogueWriter::compactSubTocIndexes() {

    // In this routine, we write out indexes that correspond to all of the data in the
    // subtoc, written by this process. Then we append a masking entry.

    Buffer buf(sizeof(TocRecord) * (fullIndexes_.size() + 1));
    size_t combinedSize = 0;

    // n.b. we only need to compact the subtocs if we are actually writing something...

    if (useSubToc() && anythingWrittenToSubToc()) {

        LOG_DEBUG_LIB(LibFdb5) << "compacting sub tocs" << std::endl;

        for (IndexStore::iterator j = fullIndexes_.begin(); j != fullIndexes_.end(); ++j) {
            Index& idx = j->second;

            if (idx.dirty()) {

                idx.flush();
                TocRecord* r = new (&buf[combinedSize]) TocRecord(serialisationVersion().used(), TocRecord::TOC_INDEX);
                combinedSize += roundRecord(*r, buildIndexRecord(*r, idx));
            }
        }

        // And add the masking record for the subtoc

        TocRecord* r = new (&buf[combinedSize]) TocRecord(serialisationVersion().used(), TocRecord::TOC_CLEAR);
        combinedSize += roundRecord(*r, buildSubTocMaskRecord(*r));

        // Write all of these  records to the toc in one go.

        appendBlock(buf, combinedSize);
    }
}


void TocCatalogueWriter::print(std::ostream &out) const {
    out << "TocCatalogueWriter(" << directory() << ")";
}

static CatalogueWriterBuilder<TocCatalogueWriter> builder("toc");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

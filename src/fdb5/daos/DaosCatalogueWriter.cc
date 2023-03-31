/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include "fdb5/fdb5_config.h"

#include "eckit/io/FileHandle.h"
// #include "eckit/config/Resource.h"
// #include "eckit/log/Log.h"
// #include "eckit/log/Bytes.h"
// #include "eckit/io/EmptyHandle.h"

// #include "fdb5/database/EntryVisitMechanism.h"
// #include "fdb5/io/FDBFileHandle.h"
#include "fdb5/LibFdb5.h"

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosKeyValueHandle.h"

// #include "fdb5/toc/TocFieldLocation.h"
#include "fdb5/daos/DaosIndex.h"
#include "fdb5/daos/DaosCatalogueWriter.h"
// #include "fdb5/io/LustreSettings.h"

// using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosCatalogueWriter::DaosCatalogueWriter(const Key &key, const fdb5::Config& config) :
    DaosCatalogue(key, config) {
    // umask_(config.umask()) {

    fdb5::DaosName pool_name{pool_};
    ASSERT(pool_name.exists());

    fdb5::DaosKeyValueName main_kv_name{pool_, root_cont_, main_kv_};
    /// @todo: just create directly?
    if (!main_kv_name.exists()) main_kv_name.create();

    fdb5::DaosSession s{};
    fdb5::DaosKeyValue main_kv{s, main_kv_name};
    fdb5::DaosKeyValueName catalogue_kv_name{pool_, db_cont_, catalogue_kv_};

    if (!main_kv.has(db_cont_)) {

        catalogue_kv_name.create();

        eckit::Log::debug<LibFdb5>() << "Copy schema from "
                                     << config_.schemaPath()
                                     << " to "
                                     << catalogue_kv_name.URI().asString()
                                     << " at key 'schema'."
                                     << std::endl;

        eckit::FileHandle in(config_.schemaPath());
        std::unique_ptr<eckit::DataHandle> out(catalogue_kv_name.dataHandle("schema"));
        in.copyTo(*out);

        std::string nstr = catalogue_kv_name.URI().asString();
        main_kv.put(db_cont_, nstr.data(), nstr.length());

    }
    
    /// @todo: record or read dbUID

    DaosCatalogue::loadSchema();

    /// @todo: TocCatalogue::checkUID();

}

DaosCatalogueWriter::DaosCatalogueWriter(const eckit::URI &uri, const fdb5::Config& config) :
    DaosCatalogue(uri, ControlIdentifiers{}, config) {
    // umask_(config.umask()) {

    NOTIMP;
    // TODO: writeInitRecord(TocCatalogue::key());
    // TODO: TocCatalogue::loadSchema();
    // TODO: TocCatalogue::checkUID();

}

DaosCatalogueWriter::~DaosCatalogueWriter() {

    clean();
    close();

}

bool DaosCatalogueWriter::selectIndex(const Key& key) {

    currentIndexKey_ = key;

    if (indexes_.find(key) == indexes_.end()) {

        fdb5::DaosKeyValueName catalogue_kv{pool_, db_cont_, catalogue_kv_};

        fdb5::DaosSession s{};
        fdb5::DaosKeyValue catalogue_kv_obj{s, catalogue_kv};

        /// @todo: use an optional, or follow a functional approach (lambda function)
        std::unique_ptr<fdb5::DaosKeyValueName> index_kv;

        if (!catalogue_kv.has(key.valuesToString())) {

            /// @todo: pass oclass from config
            /// @todo: hash string into lower oid bits
            fdb5::DaosKeyValueOID index_kv_oid{key.valuesToString(), OC_S1};
            index_kv.reset(new fdb5::DaosKeyValueName{pool_, db_cont_, index_kv_oid});
            index_kv->create();

            std::string nstr{index_kv->URI().asString()};
            catalogue_kv_obj.put(key.valuesToString(), nstr.data(), nstr.length());

        } else {

            /// @todo: address this magic value: config item called max_index_key_len
            std::vector<char> n{100};
            catalogue_kv_obj.get(key.valuesToString(), &n[0], n.size());
            index_kv.reset(new fdb5::DaosKeyValueName{eckit::URI{std::string{n.begin(), n.end()}}});

        }

        indexes_[key] = Index(new fdb5::DaosIndex(key, *index_kv));

    }

    current_ = indexes_[key];
//     current_.open();
//     current_.flock();

//     // If we are using subtocs, then we need to maintain a duplicate index that doesn't get flushed
//     // each step.

//     if (useSubToc()) {

//         if (fullIndexes_.find(key) == fullIndexes_.end()) {

//             // TODO TODO TODO .master.index
//             PathName indexPath(generateIndexPath(key));

//             // Enforce lustre striping if requested
//             if (stripeLustre()) {
//                 fdb5LustreapiFileCreate(indexPath.localPath(), stripeIndexLustreSettings());
//             }

//             fullIndexes_[key] = Index(new TocIndex(key, indexPath, 0, TocIndex::WRITE));
//         }

//         currentFull_ = fullIndexes_[key];
//         currentFull_.open();
//         currentFull_.flock();
//     }

    return true;

}

void DaosCatalogueWriter::deselectIndex() {

    current_ = Index();
    // currentFull_ = Index();
    currentIndexKey_ = Key();

}

// bool TocCatalogueWriter::open() {
//     return true;
// }

void DaosCatalogueWriter::clean() {

    // eckit::Log::debug<LibFdb5>() << "Closing path " << directory_ << std::endl;

    flush(); // closes the TOC entries & indexes but not data files

    // compactSubTocIndexes();

    deselectIndex();

}

void DaosCatalogueWriter::close() {

    closeIndexes();

}

// void TocCatalogueWriter::index(const Key &key, const eckit::URI &uri, eckit::Offset offset, eckit::Length length) {
//     dirty_ = true;

//     if (current_.null()) {
//         ASSERT(!currentIndexKey_.empty());
//         selectIndex(currentIndexKey_);
//     }

//     Field field(new TocFieldLocation(uri, offset, length, Key()), currentIndex().timestamp());

//     current_.put(key, field);

//     if (useSubToc())
//         currentFull_.put(key, field);
// }

// void TocCatalogueWriter::reconsolidateIndexesAndTocs() {

//     // TODO: This tool needs to be rewritten to reindex properly using the schema.
//     //       Currently it results in incomplete indexes if data has been written
//     //       in a context that has optional values in the schema.

//     // Visitor class for reindexing

//     class ConsolidateIndexVisitor : public EntryVisitor {
//     public:
//         ConsolidateIndexVisitor(TocCatalogueWriter& writer) :
//             writer_(writer) {}
//         ~ConsolidateIndexVisitor() override {}
//     private:
//         void visitDatum(const Field& field, const Key& key) override {
//             // TODO: Do a sneaky schema.expand() here, prepopulated with the current DB/index/Rule,
//             //       to extract the full key, including optional values.
//             const TocFieldLocation& location(static_cast<const TocFieldLocation&>(field.location()));
//             writer_.index(key, location.uri(), location.offset(), location.length());

//         }
//         void visitDatum(const Field& field, const std::string& keyFingerprint) override {
//             EntryVisitor::visitDatum(field, keyFingerprint);
//         }

//         TocCatalogueWriter& writer_;
//     };

//     // Visit all tocs and indexes

//     std::set<std::string> subtocs;
//     std::vector<bool> indexInSubtoc;
//     std::vector<Index> readIndexes = loadIndexes(false, &subtocs, &indexInSubtoc);
//     size_t maskable_indexes = 0;

//     ConsolidateIndexVisitor visitor(*this);

//     ASSERT(readIndexes.size() == indexInSubtoc.size());

//     for (size_t i = 0; i < readIndexes.size(); i++) {
//         Index& idx(readIndexes[i]);
//         selectIndex(idx.key());
//         idx.entries(visitor);

//         Log::info() << "Visiting index: " << idx.location().uri() << std::endl;

//         // We need to explicitly mask indexes in the master TOC
//         if (!indexInSubtoc[i]) maskable_indexes += 1;
//     }

//     // Flush the new indexes and add relevant entries!
//     clean();
//     close();

//     // Add masking entries for all the indexes and subtocs visited so far

//     Buffer buf(sizeof(TocRecord) * (subtocs.size() + maskable_indexes));
//     size_t combinedSize = 0;

//     for (size_t i = 0; i < readIndexes.size(); i++) {
//         // We need to explicitly mask indexes in the master TOC
//         if (!indexInSubtoc[i]) {
//             Index& idx(readIndexes[i]);
//             TocRecord* r = new (&buf[combinedSize]) TocRecord(serialisationVersion().used(), TocRecord::TOC_CLEAR);
//             combinedSize += roundRecord(*r, buildClearRecord(*r, idx));
//             Log::info() << "Masking index: " << idx.location().uri() << std::endl;
//         }
//     }

//     for (const std::string& subtoc_path : subtocs) {
//         TocRecord* r = new (&buf[combinedSize]) TocRecord(serialisationVersion().used(), TocRecord::TOC_CLEAR);
//         combinedSize += roundRecord(*r, buildSubTocMaskRecord(*r, subtoc_path));
//         Log::info() << "Masking sub-toc: " << subtoc_path << std::endl;
//     }

//     // And write all the TOC records in one go!

//     appendBlock(buf, combinedSize);
// }

const Index& DaosCatalogueWriter::currentIndex() {

    if (current_.null()) {
        ASSERT(!currentIndexKey_.empty());
        selectIndex(currentIndexKey_);
    }

    return current_;

}

// const TocSerialisationVersion& TocCatalogueWriter::serialisationVersion() const {
//     return TocHandler::serialisationVersion();
// }

// void TocCatalogueWriter::overlayDB(const Catalogue& otherCat, const std::set<std::string>& variableKeys, bool unmount) {

//     const TocCatalogue& otherCatalogue = dynamic_cast<const TocCatalogue&>(otherCat);
//     const Key& otherKey(otherCatalogue.key());

//     if (otherKey.size() != TocCatalogue::dbKey_.size()) {
//         std::stringstream ss;
//         ss << "Keys insufficiently matching for mount: " << TocCatalogue::dbKey_ << " : " << otherKey;
//         throw UserError(ss.str(), Here());
//     }

//     // Build the difference map from the old to the new key

//     for (const auto& kv : TocCatalogue::dbKey_) {

//         auto it = otherKey.find(kv.first);
//         if (it == otherKey.end()) {
//             std::stringstream ss;
//             ss << "Keys insufficiently matching for mount: " << TocCatalogue::dbKey_ << " : " << otherKey;
//             throw UserError(ss.str(), Here());
//         }

//         if (kv.second != it->second) {
//             if (variableKeys.find(kv.first) == variableKeys.end()) {
//                 std::stringstream ss;
//                 ss << "Key " << kv.first << " not allowed to differ between DBs: " << TocCatalogue::dbKey_ << " : " << otherKey;
//                 throw UserError(ss.str(), Here());
//             }
//         }
//     }

//     // And append the mount link / unmount mask
//     if (unmount) {

//         // First sanity check that we are already mounted

//         std::set<std::string> subtocs;
//         loadIndexes(false, &subtocs);

//         eckit::PathName stPath(otherCatalogue.tocPath());
//         if (subtocs.find(stPath) == subtocs.end()) {
//             std::stringstream ss;
//             ss << "Cannot unmount DB: " << otherCatalogue << ". Not currently mounted";
//             throw UserError(ss.str(), Here());
//         }

//         writeSubTocMaskRecord(otherCatalogue);
//     } else {
//         writeSubTocRecord(otherCatalogue);
//     }
// }

// void TocCatalogueWriter::hideContents() {
//     writeClearAllRecord();
// }

// bool TocCatalogueWriter::enabled(const ControlIdentifier& controlIdentifier) const {
//     if (controlIdentifier == ControlIdentifier::List || controlIdentifier == ControlIdentifier::Retrieve) {
//         return false;
//     }
//     return TocCatalogue::enabled(controlIdentifier);
// }

/// @todo: other writers may be simultaneously updating the axes KeyValues in DAOS. Should these
///        new updates be retrieved and put into in-memory axes from time to time, e.g. every
///        time a value is put in an axis KeyValue?
void DaosCatalogueWriter::archive(const Key& key, const FieldLocation* fieldLocation) {

    // dirty_ = true;

    if (current_.null()) {
        ASSERT(!currentIndexKey_.empty());
        selectIndex(currentIndexKey_);
    }

    Field field(fieldLocation, currentIndex().timestamp());

    /// before in-memory axes are updated as part of current_.put, we determine which
    /// additions will need to be performed on axes in DAOS after the field gets indexed.
    std::vector<std::string> axesToExpand;
    std::vector<std::string> valuesToAdd;

    for (Key::const_iterator i = key.begin(); i != key.end(); ++i) {

        const std::string &keyword = i->first;
        std::string value = key.canonicalValue(keyword);

        const eckit::DenseSet<std::string>& axis_set = current_.axes().valuesSafe(keyword);

        if (!axis_set.contains(value)) {

            axesToExpand.push_back(keyword);
            valuesToAdd.push_back(value);

        }

    }

    /// index the field and update in-memory axes
    current_.put(key, field);

    /// @todo: axes are supposed to be sorted before persisting. How do we do this with the DAOS approach?
    ///        sort axes every time they are loaded in the read pathway?

    if (axesToExpand.empty()) return;

    /// expand axis info in DAOS
    fdb5::DaosSession s{};
    while (!axesToExpand.empty()) {

        /// @todo: take oclass from config
        fdb5::DaosKeyValueOID oid{currentIndexKey_.valuesToString() + std::string{"."} + axesToExpand.back(), OC_S1};
        fdb5::DaosKeyValueName n{pool_, db_cont_, oid};
        fdb5::DaosKeyValue kv{s, n};
        std::string v{"1"};
        kv.put(valuesToAdd.back(), v.data(), v.length());
        axesToExpand.pop_back();
        valuesToAdd.pop_back();

    }

    // if (useSubToc())
    //     currentFull_.put(key, field);

}

void DaosCatalogueWriter::flush() {
    // if (!dirty_) {
    //     return;
    // }

//     flushIndexes();

    if (!current_.null()) current_ = Index();

    // dirty_ = false;
    // current_ = Index();
    // currentFull_ = Index();
}

// eckit::PathName TocCatalogueWriter::generateIndexPath(const Key &key) const {
//     eckit::PathName tocPath ( directory_ );
//     tocPath /= key.valuesToString();
//     tocPath = eckit::PathName::unique(tocPath) + ".index";
//     return tocPath;
// }

// // n.b. We do _not_ flush the fullIndexes_ set of indexes.
// // The indexes pointed to in the indexes_ list get written out each time there is
// // a flush (i.e. every step). The indexes stored in fullIndexes then contain _all_
// // the data that is indexes thorughout the lifetime of the DBWriter, which can be
// // compacted later for read performance.
// void TocCatalogueWriter::flushIndexes() {
//     for (IndexStore::iterator j = indexes_.begin(); j != indexes_.end(); ++j ) {
//         Index& idx = j->second;

//         if (idx.dirty()) {
//             idx.flush();
//             writeIndexRecord(idx);
//             idx.reopen(); // Create a new btree
//         }
//     }
// }

void DaosCatalogueWriter::closeIndexes() {
    // for (IndexStore::iterator j = indexes_.begin(); j != indexes_.end(); ++j ) {
    //     Index& idx = j->second;
    //     idx.close();
    // }

    // for (IndexStore::iterator j = fullIndexes_.begin(); j != fullIndexes_.end(); ++j ) {
    //     Index& idx = j->second;
    //     idx.close();
    // }

    indexes_.clear(); // all indexes instances destroyed
    // fullIndexes_.clear(); // all indexes instances destroyed
}

// void TocCatalogueWriter::compactSubTocIndexes() {

//     // In this routine, we write out indexes that correspond to all of the data in the
//     // subtoc, written by this process. Then we append a masking entry.

//     Buffer buf(sizeof(TocRecord) * (fullIndexes_.size() + 1));
//     size_t combinedSize = 0;

//     // n.b. we only need to compact the subtocs if we are actually writing something...

//     if (useSubToc() && anythingWrittenToSubToc()) {

//         eckit::Log::debug<LibFdb5>() << "compacting sub tocs" << std::endl;

//         for (IndexStore::iterator j = fullIndexes_.begin(); j != fullIndexes_.end(); ++j) {
//             Index& idx = j->second;

//             if (idx.dirty()) {

//                 idx.flush();
//                 TocRecord* r = new (&buf[combinedSize]) TocRecord(serialisationVersion().used(), TocRecord::TOC_INDEX);
//                 combinedSize += roundRecord(*r, buildIndexRecord(*r, idx));
//             }
//         }

//         // And add the masking record for the subtoc

//         TocRecord* r = new (&buf[combinedSize]) TocRecord(serialisationVersion().used(), TocRecord::TOC_CLEAR);
//         combinedSize += roundRecord(*r, buildSubTocMaskRecord(*r));

//         // Write all of these  records to the toc in one go.

//         appendBlock(buf, combinedSize);
//     }
// }


// void TocCatalogueWriter::print(std::ostream &out) const {
//     out << "TocCatalogueWriter(" << directory() << ")";
// }

static fdb5::CatalogueBuilder<fdb5::DaosCatalogueWriter> builder("daos.writer");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

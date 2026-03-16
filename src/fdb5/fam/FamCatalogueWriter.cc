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

#include "fdb5/fam/FamCatalogueWriter.h"

#include <climits>

#include "eckit/io/MemoryHandle.h"
#include "eckit/io/fam/FamMap.h"
#include "eckit/io/fam/FamRegion.h"
#include "eckit/serialisation/HandleStream.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/fam/FamIndex.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {

std::string serializeKey(const fdb5::Key& key) {
    eckit::MemoryHandle h{static_cast<size_t>(PATH_MAX)};
    eckit::HandleStream hs{h};
    h.openForWrite(0);
    {
        eckit::AutoClose c(h);
        hs << key;
    }
    return {static_cast<const char*>(h.data()), static_cast<std::size_t>(hs.bytesWritten())};
}

const fdb5::CatalogueWriterBuilder<fdb5::FamCatalogueWriter> fam_cat_writer_builder(FamCommon::type);

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

FamCatalogueWriter::FamCatalogueWriter(const Key& key, const fdb5::Config& config) : FamCatalogue(key, config) {
    initCatalogue();
}

FamCatalogueWriter::FamCatalogueWriter(const eckit::URI& uri, const fdb5::Config& config) :
    FamCatalogue(uri, ControlIdentifiers{}, config) {
    NOTIMP;
}

FamCatalogueWriter::~FamCatalogueWriter() {
    clean();
    close();
}

//----------------------------------------------------------------------------------------------------------------------

void FamCatalogueWriter::initCatalogue() {

    auto region = root_.lookup();

    const auto db_key = serializeKey(dbKey_);

    // idempotent (FamMap::insert is a no-op if key exists)

    // Register this DB in the global FDB registry
    Map(FamCommon::registry_name, region).insert(FamCommon::toString(dbKey_), db_key);

    // Create / open the per-DB catalogue map and store the DB key
    Map(name(), region).insert(FamCommon::db_key, db_key);

    FamCatalogue::loadSchema();
}

//----------------------------------------------------------------------------------------------------------------------

void FamCatalogueWriter::index(const Key& /*key*/, const eckit::URI& /*uri*/, eckit::Offset /*offset*/,
                               eckit::Length /*length*/) {
    NOTIMP;
}

void FamCatalogueWriter::overlayDB(const Catalogue& /*other_catalogue*/, const std::set<std::string>& /*variable_keys*/,
                                   bool /*unmount*/) {
    NOTIMP;
}

void FamCatalogueWriter::reconsolidate() {
    NOTIMP;
}

bool FamCatalogueWriter::open() {
    NOTIMP;
}

bool FamCatalogueWriter::createIndex(const Key& /*idx_key*/, size_t /*datum_key_size*/) {
    return true;  // creation is handled lazily in selectIndex
}

bool FamCatalogueWriter::selectIndex(const Key& idx_key) {
    // Fast path: already selected.
    if (FamCatalogue::selectIndex(idx_key)) {
        return true;
    }
    // found in the cache
    if (auto iter = indexes_.find(idx_key); iter != indexes_.end()) {
        current_ = iter->second;
        return true;
    }
    // Create or open the FamIndex for this key.
    current_ = Index(new FamIndex(idx_key, *this, root_, indexName(idx_key), false));
    // cache it for future selectIndex calls
    indexes_[idx_key] = current_;

    // Register this index in the fam catalogue map
    const auto region = root_.lookup();
    Map(name(), region).insert(FamCommon::toString(idx_key), serializeKey(idx_key));

    return true;
}

void FamCatalogueWriter::deselectIndex() {
    FamCatalogue::deselectIndex();
    current_ = Index();
}

void FamCatalogueWriter::clean() {
    flush(0);
    deselectIndex();
}

void FamCatalogueWriter::close() {}  // FAM is persistent; nothing to close.

const Index& FamCatalogueWriter::currentIndex() {
    if (current_.null()) {
        ASSERT(!indexKey().empty());
        selectIndex(indexKey());
    }
    return current_;
}

void FamCatalogueWriter::archive(const Key& idx_key, const Key& datum_key,
                                 std::shared_ptr<const FieldLocation> field_location) {

    selectIndex(idx_key);

    Field field(std::move(field_location), current_.timestamp());
    current_.put(datum_key, field);
}

void FamCatalogueWriter::flush(size_t /*archivedFields*/) {
    LOG_DEBUG_LIB(LibFdb5) << "FamCatalogueWriter::flush" << std::endl;
    // Sort axes once per flush, not on every archive() call.
    for (auto& [key, idx] : indexes_) {
        const_cast<fdb5::IndexAxis&>(idx.axes()).sort();
    }
}

void FamCatalogueWriter::print(std::ostream& out) const {
    out << "FamCatalogueWriter[" << uri() << "]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

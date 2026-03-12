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

#include "eckit/io/AutoCloser.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/fam/FamMap.h"
#include "eckit/io/fam/FamMapEntry.h"
#include "eckit/io/fam/FamRegion.h"
#include "eckit/serialisation/HandleStream.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/fam/FamIndex.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

using Map = eckit::FamMap<eckit::FamMapEntry<128>>;

namespace {

/// Name of the FDB-global registry map within the root region.
constexpr const char* REGISTRY_MAP_NAME = "fdb-reg";

/// Special key under which the serialised DB key is stored in the catalogue map.
constexpr const char* KEY_DBKEY = "__dbkey__";

std::string serializeKey(const fdb5::Key& key) {
    eckit::MemoryHandle h{static_cast<size_t>(PATH_MAX)};
    eckit::HandleStream hs{h};
    h.openForWrite(0);
    {
        eckit::AutoClose c(h);
        hs << key;
    }
    return std::string(static_cast<const char*>(h.data()), static_cast<std::size_t>(hs.bytesWritten()));
}

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

    eckit::FamRegion region = root_.lookup();

    // Register this DB in the global FDB registry (idempotent: FamMap::insert is a no-op if key exists).
    {
        Map reg_map(REGISTRY_MAP_NAME, region);
        const std::string encoded = serializeKey(dbKey_);
        reg_map.insert(Map::key_type{FamCommon::toString(dbKey_)}, encoded.data(), encoded.size());
    }

    // Create / open the per-DB catalogue map and store the DB key (idempotent).
    {
        Map cat_map(name(), region);
        const std::string encoded = serializeKey(dbKey_);
        cat_map.insert(Map::key_type{KEY_DBKEY}, encoded.data(), encoded.size());
    }

    FamCatalogue::loadSchema();
}

//----------------------------------------------------------------------------------------------------------------------

bool FamCatalogueWriter::createIndex(const Key& /*idxKey*/, size_t /*datumKeySize*/) {
    return true;  // creation is handled lazily in selectIndex
}

bool FamCatalogueWriter::selectIndex(const Key& idx_key) {
    // Fast path: already selected.
    if (FamCatalogue::selectIndex(idx_key)) {
        return true;
    }

    auto iter = indexes_.find(idx_key);
    if (iter != indexes_.end()) {
        current_ = iter->second;
        return true;
    }

    const auto map_name = indexName(FamCommon::toString(idx_key));

    // Create or open the FamIndex.
    current_          = Index(new FamIndex(idx_key, *this, root_, map_name, /*readAxes=*/false));
    indexes_[idx_key] = current_;

    // Register this index in the catalogue map (idempotent: FamMap::insert is a no-op if idx_key exists).
    eckit::FamRegion region = root_.lookup();
    Map cat_map(name(), region);
    const std::string encoded = serializeKey(idx_key);
    cat_map.insert(Map::key_type{FamCommon::toString(idx_key)}, encoded.data(), encoded.size());

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

void FamCatalogueWriter::archive(const Key& idxKey, const Key& datumKey,
                                 std::shared_ptr<const FieldLocation> fieldLocation) {

    selectIndex(idxKey);

    Field field(std::move(fieldLocation), current_.timestamp());
    current_.put(datumKey, field);
}

void FamCatalogueWriter::flush(size_t /*archivedFields*/) {
    LOG_DEBUG_LIB(LibFdb5) << "FamCatalogueWriter::flush" << std::endl;
    // Sort axes once per flush, not on every archive() call.
    for (auto& [key, idx] : indexes_) {
        const_cast<fdb5::IndexAxis&>(idx.axes()).sort();
    }
}

static fdb5::CatalogueWriterBuilder<fdb5::FamCatalogueWriter> famCatalogueWriterBuilder("fam");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

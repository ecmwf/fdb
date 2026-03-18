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

#include <fstream>
#include <sstream>

#include "eckit/io/fam/FamMap.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/fam/FamIndex.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {

const fdb5::CatalogueWriterBuilder<fdb5::FamCatalogueWriter> fam_cat_writer_builder(FamCommon::type);

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

FamCatalogueWriter::FamCatalogueWriter(const Key& key, const fdb5::Config& config) : FamCatalogue(key, config) {
    initCatalogue();
}

FamCatalogueWriter::FamCatalogueWriter(const eckit::URI& uri, const fdb5::Config& config) :
    FamCatalogue(uri, ControlIdentifiers{}, config) {
    initCatalogue();
}

FamCatalogueWriter::~FamCatalogueWriter() {
    clean();
}

//----------------------------------------------------------------------------------------------------------------------

void FamCatalogueWriter::dumpSchema(std::ostream& stream) const {
    auto& cat = catalogue();
    // 1- If schema exists on FAM, dump it to the stream.
    if (auto iter = cat.find(schema_keyword); iter != cat.end()) {
        auto schema = (*iter).value;
        stream << schema.view();
        return;
    }
    // 2- Not on FAM, read local schema and persist it in FAM.
    std::ifstream file(config_.schemaPath());
    if (!file) {
        throw eckit::CantOpenFile(config_.schemaPath());
    }
    std::ostringstream ss;
    ss << file.rdbuf();
    const std::string schema = ss.str();
    // persist the schema in the FAM catalogue
    cat.insert(schema_keyword, schema);
    stream << schema;
}


void FamCatalogueWriter::initCatalogue() {
    const auto key = encodeKey(dbKey_);

    // Register this DB in the global FDB registry
    Map(registry_keyword, getRegion()).insert(toString(dbKey_), key);

    // Create / open the per-DB catalogue map and store the DB key.
    catalogue().insert(db_keyword, key);

    loadSchema();
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
    return true;  // nothing to open for FAM catalogue
}

bool FamCatalogueWriter::createIndex(const Key& /*idx_key*/, size_t /*datum_key_size*/) {
    return true;  // creation is handled lazily in selectIndex
}

bool FamCatalogueWriter::selectIndex(const Key& key) {
    // Fast path: already selected.
    if (FamCatalogue::selectIndex(key)) {
        return true;
    }
    // found in the cache
    if (auto iter = indexes_.find(key); iter != indexes_.end()) {
        current_ = iter->second;
        return true;
    }
    // Create or open the FamIndex for this key.
    current_ = Index(new FamIndex(key, root_, indexName(key), false));
    current_.open();
    // cache it for future selectIndex calls
    indexes_[key] = current_;

    // Register this index in the catalogue FamMap.  The "i:" prefix distinguishes
    // index entries from administrative sentinel keys (e.g. "__fdb__").
    catalogue().insert("i:" + toString(key), encodeKey(key));

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
    // Delegate axis sorting to FamIndex::flush(), keeping the writer free of casts.
    for (auto& [key, idx] : indexes_) {
        idx.flush();
    }
}

void FamCatalogueWriter::print(std::ostream& out) const {
    out << "FamCatalogueWriter[" << uri() << "]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

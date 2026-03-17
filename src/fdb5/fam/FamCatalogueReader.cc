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

#include "fdb5/fam/FamCatalogueReader.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/fam/FamIndex.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

namespace {

const fdb5::CatalogueReaderBuilder<fdb5::FamCatalogueReader> fam_cat_reader_builder(FamCommon::type);

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

FamCatalogueReader::FamCatalogueReader(const Key& key, const fdb5::Config& config) : FamCatalogue(key, config) {}

FamCatalogueReader::FamCatalogueReader(const eckit::URI& uri, const fdb5::Config& config) :
    FamCatalogue(uri, ControlIdentifiers{}, config) {}

//----------------------------------------------------------------------------------------------------------------------

bool FamCatalogueReader::selectIndex(const Key& key) {
    if (FamCatalogue::selectIndex(key)) {
        return true;
    }

    auto iter = indexes_.find(key);
    if (iter != indexes_.end()) {
        current_ = iter->second;
        return true;
    }

    const std::string map_name = indexName(key);

    // Check the index map exists: the table object must be present in the region.
    if (!root_.object(map_name + FamCommon::table_suffix).exists()) {
        return false;
    }

    indexes_[key] = Index(new FamIndex(key, *this, root_, map_name, true));
    current_ = indexes_[key];
    return true;
}

void FamCatalogueReader::deselectIndex() {
    FamCatalogue::deselectIndex();
    current_ = Index();
}

bool FamCatalogueReader::open() {
    if (!FamCatalogue::exists()) {
        return false;
    }
    loadSchema();
    return true;
}

void FamCatalogueReader::dumpSchema(std::ostream& stream) const {
    auto cat = catalogue();
    auto iter = cat.find(FamCommon::schema_key);
    if (iter == cat.end()) {
        throw eckit::BadValue("FamCatalogueReader: schema not found in catalogue at: " + uri().asString());
    }
    auto schema = (*iter).value.view();
    stream << schema;
}

std::optional<Axis> FamCatalogueReader::computeAxis(const std::string& keyword) const {
    Axis axis;
    bool found = false;
    for (const auto& [key, index] : indexes_) {
        if (index.axes().has(keyword)) {
            axis.merge(index.axes().values(keyword));
            found = true;
        }
    }
    if (found) {
        return axis;
    }
    return {};
}

bool FamCatalogueReader::retrieve(const Key& key, Field& field) const {
    LOG_DEBUG_LIB(LibFdb5) << "FamCatalogueReader::retrieve key=" << key << std::endl;
    if (current_.null() || !current_.mayContain(key)) {
        return false;
    }
    return current_.get(key, Key(), field);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

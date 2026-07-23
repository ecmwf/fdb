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
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Index.h"
#include "fdb5/fam/FamCatalogue.h"
#include "fdb5/fam/FamCommon.h"
#include "fdb5/fam/FamIndex.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/log/Log.h"

#include <optional>
#include <ostream>
#include <string>

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
    // Fast path: same key already selected and its index object is cached.
    if (FamCatalogue::selectIndex(key) && !current_.null()) {
        return true;
    }

    auto iter = indexes_.find(key);
    if (iter != indexes_.end()) {
        current_ = iter->second;
        return true;
    }

    const auto index_name = indexName(key);

    // Check the index map exists: the table object must be present in the region.
    if (!tableObject(index_name).exists()) {
        deselectIndex();
        return false;
    }

    const auto& region_name = root();
    indexes_[key] = Index(new FamIndex(key, region_name, index_name, true));
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
    const auto& cat = catalogue();
    auto iter = cat.find(schema_keyword);
    if (iter == cat.end()) {
        throw eckit::BadValue("FamCatalogueReader: schema not found in catalogue at: " + uri().asString());
    }
    auto schema = (*iter).value;
    stream << schema.view();
}

std::optional<Axis> FamCatalogueReader::computeAxis(const std::string& keyword) const {
    if (current_.null()) {
        return std::nullopt;
    }
    if (!current_.axes().has(keyword)) {
        return std::nullopt;
    }
    Axis axis;
    axis.merge(current_.axes().values(keyword));
    return axis;
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

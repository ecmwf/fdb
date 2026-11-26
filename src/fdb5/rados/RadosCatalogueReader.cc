// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/rados/RadosCatalogueReader.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/DbStats.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/rados/RadosCatalogue.h"
#include "fdb5/rados/RadosCommon.h"
#include "fdb5/rados/RadosIndex.h"
#include "fdb5/rados/RadosStats.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/rados/RadosException.h"
#include "eckit/io/rados/RadosKeyValue.h"
#include "eckit/log/Log.h"

#include <optional>
#include <ostream>
#include <string>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RadosCatalogueReader::RadosCatalogueReader(const Key& key, const Config& config) : RadosCatalogue(key, config) {}

RadosCatalogueReader::RadosCatalogueReader(const eckit::URI& uri, const Config& config) :
    RadosCatalogue(uri, ControlIdentifiers{}, config) {}

bool RadosCatalogueReader::selectIndex(const Key& key) {

    if (currentIndexKey_ == key) {
        return true;
    }

    if (indexes_.find(key) == indexes_.end()) {
        const auto index_kv = registeredIndex(key);
        if (!index_kv) {
            return false;
        }
        indexes_[key] = Index(new RadosIndex(key, *index_kv, true));
    }

    // The axis cache of CatalogueReader holds the axes of the previously selected index.
    invalidateAxis();

    currentIndexKey_ = key;
    current_ = indexes_[key];

    return true;
}

void RadosCatalogueReader::deselectIndex() {
    invalidateAxis();
    current_ = Index();
    currentIndexKey_ = Key();
}

bool RadosCatalogueReader::open() {
    if (!RadosCatalogue::exists()) {
        return false;
    }
    RadosCatalogue::loadSchema();
    return true;
}

DbStats RadosCatalogueReader::stats() const {

    auto* content = new RadosDbStats();
    content->dbCount_ = 1;

    for (const auto& indexEntry : indexes(false)) {
        content->indexCount_++;
        const auto* radosIndex = dynamic_cast<const RadosIndex*>(indexEntry.content());
        ASSERT(radosIndex);
        for (const auto& key : radosIndex->idx_kv().keys()) {
            if (rados_names::has_prefix(key, rados_names::fieldPrefix)) {
                content->fieldCount_++;
            }
        }
    }

    return DbStats(content);
}

std::optional<Axis> RadosCatalogueReader::computeAxis(const std::string& keyword) const {

    if (!current_.axes().has(keyword)) {
        return std::nullopt;
    }

    Axis s;
    s.merge(current_.axes().values(keyword));
    return s;
}

bool RadosCatalogueReader::retrieve(const Key& key, Field& field) const {

    eckit::Log::debug<LibFdb5>() << "Trying to retrieve key " << key << std::endl;
    eckit::Log::debug<LibFdb5>() << "Scanning index " << current_.location() << std::endl;

    if (!current_.mayContain(key)) {
        return false;
    }

    return current_.get(key, Key(), field);
}

static CatalogueReaderBuilder<RadosCatalogueReader> builder("rados");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/rados/RadosCatalogueReader.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Key.h"
#include "fdb5/rados/RadosCatalogue.h"
#include "fdb5/rados/RadosIndex.h"

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
        try {
            std::vector<char> data;
            db_kv_->getMemoryStream(data, key.valuesToString(), "DB kv");
            eckit::URI uri{std::string{data.begin(), data.end()}};
            eckit::RadosKeyValue index_kv{uri};
            indexes_[key] = Index(new RadosIndex(key, index_kv, true));
        }
        catch (eckit::RadosEntityNotFoundException& e) {
            return false;
        }
    }

    currentIndexKey_ = key;
    current_ = indexes_[key];

    return true;
}

void RadosCatalogueReader::deselectIndex() {
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

std::optional<Axis> RadosCatalogueReader::computeAxis(const std::string& keyword) const {

    Axis s;

    bool found = false;
    if (current_.axes().has(keyword)) {
        found = true;
        s.merge(current_.axes().values(keyword));
    }

    if (found) {
        return s;
    }
    return std::nullopt;
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

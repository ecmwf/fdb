/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/rados/RadosCatalogueWriter.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Field.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/IndexAxis.h"
#include "fdb5/database/Key.h"
#include "fdb5/rados/RadosCatalogue.h"
#include "fdb5/rados/RadosCleanup.h"
#include "fdb5/rados/RadosIndex.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/FileHandle.h"
#include "eckit/io/Length.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/io/rados/RadosException.h"
#include "eckit/io/rados/RadosKeyValue.h"
#include "eckit/io/rados/RadosNamespace.h"
#include "eckit/log/Log.h"
#include "eckit/serialisation/HandleStream.h"

#include <climits>
#include <cstddef>
#include <exception>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

RadosCatalogueWriter::RadosCatalogueWriter(const Key& key, const fdb5::Config& config) :
    RadosCatalogue(key, config), firstIndexWrite_(false) {

    std::string db_name = db_namespace_;
    ASSERT(root_kv_->nspace().pool().exists());

    root_kv_->ensureCreated();
    if (!root_kv_->has(db_name)) {

        db_kv_->ensureCreated();

        eckit::Log::debug<LibFdb5>() << "Copy schema from " << config_.schemaPath() << " to "
                                     << db_kv_->uri().asString() << " at key 'schema'." << std::endl;

        eckit::FileHandle in(config_.schemaPath());
        std::vector<char> data;
        data.resize(in.size());
        {
            eckit::AutoClose ac{in};
            in.openForRead();
            in.read(&data[0], in.size());
        }
        db_kv_->put("schema", &data[0], data.size());

        eckit::MemoryHandle h{(size_t)PATH_MAX};
        eckit::HandleStream hs{h};
        h.openForWrite(eckit::Length(0));
        {
            eckit::AutoClose closer(h);
            hs << dbKey_;
        }

        db_kv_->put("key", h.data(), hs.bytesWritten());

        std::string nstr = db_kv_->uri().asString();
        root_kv_->put(db_name, nstr.data(), nstr.length());
    }

    RadosCatalogue::loadSchema();

    /// @todo: TocCatalogue::checkUID();
}

RadosCatalogueWriter::RadosCatalogueWriter(const eckit::URI& uri, const fdb5::Config& config) :
    RadosCatalogue(uri, ControlIdentifiers{}, config), firstIndexWrite_(false) {
    RadosCatalogue::loadSchema();
}

RadosCatalogueWriter::~RadosCatalogueWriter() {
    std::exception_ptr ignored;
    best_effort(ignored, "~RadosCatalogueWriter::clean", [&] { clean(); });
    best_effort(ignored, "~RadosCatalogueWriter::close", [&] { close(); });
}

bool RadosCatalogueWriter::createIndex(const Key& /* idxKey */, size_t /* datumKeySize */) {
    return true;
}

bool RadosCatalogueWriter::selectIndex(const Key& key) {

    currentIndexKey_ = key;

    if (indexes_.find(key) == indexes_.end()) {

        try {
            std::vector<char> data;
            db_kv_->getMemoryStream(data, key.valuesToString(), "DB kv");

            indexes_[key] = Index(new fdb5::RadosIndex(
                key, eckit::RadosKeyValue{eckit::URI{std::string{data.begin(), data.end()}}}, false));
        }
        catch (eckit::RadosEntityNotFoundException& e) {

            firstIndexWrite_ = true;

            indexes_[key] = Index(new fdb5::RadosIndex(key, eckit::RadosNamespace{pool_, db_namespace_}));

            /// index index kv in catalogue kv
            std::string nstr{indexes_[key].location().uri().asString()};
            db_kv_->put(key.valuesToString(), nstr.data(), nstr.length());
        }
    }

    current_ = indexes_[key];

    return true;
}

void RadosCatalogueWriter::deselectIndex() {
    current_ = Index();
    currentIndexKey_ = Key();
    firstIndexWrite_ = false;
}

void RadosCatalogueWriter::clean() {
    flush(0);
    deselectIndex();
}

void RadosCatalogueWriter::close() {
    closeIndexes();
}

const Index& RadosCatalogueWriter::currentIndex() {
    if (current_.null()) {
        ASSERT(!currentIndexKey_.empty());
        selectIndex(currentIndexKey_);
    }
    return current_;
}

void RadosCatalogueWriter::archive(const Key& /* idxKey */, const Key& datumKey,
                                   std::shared_ptr<const FieldLocation> fieldLocation) {

    if (current_.null()) {
        ASSERT(!currentIndexKey_.empty());
        selectIndex(currentIndexKey_);
    }

    Field field(std::move(fieldLocation), currentIndex().timestamp());

    const_cast<fdb5::IndexAxis&>(current_.axes()).sort();

    std::vector<std::string> axesToExpand;
    std::vector<std::string> valuesToAdd;
    std::string axisNames;
    std::string sep;

    for (const auto& [keyword, value] : datumKey) {

        if (value.length() == 0) {
            continue;
        }

        axisNames += sep + keyword;
        sep = ",";

        const auto& axis_set = current_.axes().values(keyword);

        if (!axis_set.contains(value)) {

            axesToExpand.push_back(keyword);
            valuesToAdd.push_back(value);
        }
    }

    current_.put(datumKey, field);

    auto* radosIndex = dynamic_cast<fdb5::RadosIndex*>(current_.content());
    ASSERT(radosIndex);

    if (firstIndexWrite_) {
        radosIndex->putAxisNames(axisNames);
        firstIndexWrite_ = false;
    }

    while (!axesToExpand.empty()) {
        radosIndex->putAxisValue(axesToExpand.back(), valuesToAdd.back());
        axesToExpand.pop_back();
        valuesToAdd.pop_back();
    }
}

void RadosCatalogueWriter::flush(size_t /* archivedFields */) {
    if (!current_.null()) {
        current_ = Index();
    }
}

void RadosCatalogueWriter::closeIndexes() {
    indexes_.clear();
}

static fdb5::CatalogueWriterBuilder<fdb5::RadosCatalogueWriter> builder("rados");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

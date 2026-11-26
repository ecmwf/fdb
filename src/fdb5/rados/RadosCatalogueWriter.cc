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
#include "fdb5/rados/RadosCommon.h"
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

RadosCatalogueWriter::RadosCatalogueWriter(const Key& key, const fdb5::Config& config) : RadosCatalogue(key, config) {

    ASSERT(root_kv_->nspace().pool().exists());

    const bool exists = db_kv_->has("key");

    // A registry entry alone does not prove the database exists: a wipe interrupted after destroying the
    // namespace leaves the entry behind, and the database must then be created again.
    created_ = exists && root_kv_->has(db_namespace_);

    if (exists) {
        RadosCatalogue::loadSchema();
        return;
    }

    // A new database is only written to RADOS by the first flush(). Until then the configured schema is used.
    eckit::FileHandle in(config_.schemaPath());
    schemaText_.resize(in.size());
    {
        eckit::AutoClose ac{in};
        in.openForRead();
        ASSERT(in.read(schemaText_.data(), schemaText_.size()) == static_cast<long>(schemaText_.size()));
    }

    RadosCatalogue::loadSchema(schemaText_);
}

void RadosCatalogueWriter::createDatabase() {

    if (!db_kv_->has("key")) {

        eckit::Log::debug<LibFdb5>() << "Copy schema from " << config_.schemaPath() << " to "
                                     << db_kv_->uri().asString() << " at key 'schema'." << std::endl;

        db_kv_->put("schema", schemaText_.data(), schemaText_.size());

        eckit::MemoryHandle h{(size_t)PATH_MAX};
        eckit::HandleStream hs{h};
        h.openForWrite(eckit::Length(0));
        {
            eckit::AutoClose closer(h);
            hs << dbKey_;
        }

        // Written after the schema: readers treat a catalogue KV without "key" as a missing database.
        db_kv_->put("key", h.data(), hs.bytesWritten());
    }

    const std::string nstr = db_kv_->uri().asString();
    root_kv_->put(db_namespace_, nstr.data(), nstr.length());

    created_ = true;
}

RadosCatalogueWriter::RadosCatalogueWriter(const eckit::URI& uri, const fdb5::Config& config) :
    RadosCatalogue(uri, ControlIdentifiers{}, config) {
    RadosCatalogue::loadSchema();
}

RadosCatalogueWriter::~RadosCatalogueWriter() {
    // Entries which were not flushed are dropped: the data they refer to may not have been flushed either.
    std::exception_ptr ignored;
    best_effort(ignored, "~RadosCatalogueWriter::close", [&] { close(); });
}

bool RadosCatalogueWriter::createIndex(const Key& /* idxKey */, size_t /* datumKeySize */) {
    return true;
}

void RadosCatalogueWriter::hideContents() {
    control(ControlAction::Disable, ControlIdentifier::List | ControlIdentifier::Retrieve);
}

bool RadosCatalogueWriter::selectIndex(const Key& key) {

    currentIndexKey_ = key;

    if (indexes_.find(key) == indexes_.end()) {

        try {
            std::vector<char> data;
            db_kv_->getMemoryStream(data, rados_names::indexPrefix + key.valuesToString(), "DB kv");

            indexes_[key] = Index(new fdb5::RadosIndex(
                key, eckit::RadosKeyValue{eckit::URI{std::string{data.begin(), data.end()}}}, false));
        }
        catch (eckit::RadosEntityNotFoundException& e) {

            // The index KV is written and registered in the catalogue KV by the first flush().
            indexes_[key] = Index(new fdb5::RadosIndex(key, eckit::RadosNamespace{pool_, db_namespace_}));
            unregistered_.insert(key);
        }
    }

    current_ = indexes_[key];

    return true;
}

void RadosCatalogueWriter::deselectIndex() {
    current_ = Index();
    currentIndexKey_ = Key();
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

    for (const auto& [keyword, value] : datumKey) {

        if (value.length() == 0) {
            continue;
        }

        const auto& axis_set = current_.axes().values(keyword);

        if (!axis_set.contains(value)) {

            axesToExpand.push_back(keyword);
            valuesToAdd.push_back(value);
        }
    }

    current_.put(datumKey, field);

    auto* radosIndex = dynamic_cast<fdb5::RadosIndex*>(current_.content());
    ASSERT(radosIndex);

    while (!axesToExpand.empty()) {
        radosIndex->putAxisValue(axesToExpand.back(), valuesToAdd.back());
        axesToExpand.pop_back();
        valuesToAdd.pop_back();
    }
}

void RadosCatalogueWriter::flush(size_t /* archivedFields */) {

    for (auto& [key, index] : indexes_) {

        if (!index.dirty()) {
            continue;
        }

        if (!created_) {
            createDatabase();
        }

        index.flush();

        // Registered last, so readers only find an index whose KV is complete.
        if (unregistered_.erase(key) > 0) {
            const std::string nstr{index.location().uri().asString()};
            db_kv_->put(rados_names::indexPrefix + key.valuesToString(), nstr.data(), nstr.length());
        }
    }

    current_ = Index();
}

void RadosCatalogueWriter::closeIndexes() {
    indexes_.clear();
    unregistered_.clear();
}

static fdb5::CatalogueWriterBuilder<fdb5::RadosCatalogueWriter> builder("rados");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

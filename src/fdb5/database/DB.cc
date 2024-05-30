/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/utils/StringTools.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/Field.h"
#include "fdb5/toc/TocEngine.h"
#include "fdb5/api/helpers/ArchiveCallback.h"

using eckit::Log;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

std::unique_ptr<DB> DB::buildReader(const Key &key, const fdb5::Config& config) {
    return std::unique_ptr<DB>(new DB(key, config, true));
}
std::unique_ptr<DB> DB::buildWriter(const Key &key, const fdb5::Config& config) {
    return std::unique_ptr<DB>(new DB(key, config, false));
}
std::unique_ptr<DB> DB::buildReader(const eckit::URI& uri, const fdb5::Config& config) {
    return std::unique_ptr<DB>(new DB(uri, config, true));
}
std::unique_ptr<DB> DB::buildWriter(const eckit::URI& uri, const fdb5::Config& config) {
    return std::unique_ptr<DB>(new DB(uri, config, false));
}

DB::DB(const Key& key, const fdb5::Config& config, bool read) {
    catalogue_ = CatalogueFactory::instance().build(key, config.expandConfig(), read);
}

DB::DB(const eckit::URI& uri, const fdb5::Config& config, bool read) {
    catalogue_ = CatalogueFactory::instance().build(uri, config.expandConfig(), read);
}

Store& DB::store() const {
    if (store_ == nullptr) {
        store_ = catalogue_->buildStore();
    }

    return *store_;
}

std::string DB::dbType() const {
    return catalogue_->type();// + ":" + store_->type();
}

const Key& DB::key() const {
    return catalogue_->key();
}
const Key& DB::indexKey() const {
    return catalogue_->indexKey();
}
const Schema& DB::schema() const {
    return catalogue_->schema();
}

bool DB::selectIndex(const Key &key) {
    return catalogue_->selectIndex(key);
}

void DB::deselectIndex() {
    return catalogue_->deselectIndex();
}

void DB::visitEntries(EntryVisitor& visitor, bool sorted) {
    catalogue_->visitEntries(visitor, store(), sorted);
}


bool DB::axis(const std::string &keyword, eckit::StringSet &s) const {
    CatalogueReader* cat = dynamic_cast<CatalogueReader*>(catalogue_.get());
    ASSERT(cat);
    return cat->axis(keyword, s);
}

bool DB::inspect(const Key& key, Field& field) {

    LOG_DEBUG_LIB(LibFdb5) << "Trying to retrieve key " << key << std::endl;

    CatalogueReader* cat = dynamic_cast<CatalogueReader*>(catalogue_.get());
    ASSERT(cat);

    return cat->retrieve(key, field);
}

eckit::DataHandle *DB::retrieve(const Key& key) {

    Field field;
    if (inspect(key, field)) {
        return store().retrieve(field);
    }

    return nullptr;
}

void DB::archive(const Key& key, const void* data, eckit::Length length, const Key& internalKey, ArchiveCallback callback) {

    CatalogueWriter* cat = dynamic_cast<CatalogueWriter*>(catalogue_.get());
    ASSERT(cat);

    const Index& idx = cat->currentIndex();
    std::unique_ptr<FieldLocation> location(store().archive(idx.key(), data, length));

    if (callback) {
        callback(internalKey, *location);
    }

    cat->archive(key, std::move(location));
}

bool DB::open() {
    bool ret = catalogue_->open();
    if (!ret)
            return ret;

    return store().open();
}

void DB::flush() {
    if (store_ != nullptr)
        store_->flush();
    catalogue_->flush();
}

void DB::close() {
    flush();
    catalogue_->clean();
    if (store_ != nullptr)
        store_->close();
    catalogue_->close();
}

bool DB::exists() const {
    return (catalogue_->exists()/* && store_->exists()*/);
}

void DB::hideContents() {
    if (catalogue_->type() == TocEngine::typeName()) {
        catalogue_->hideContents();
    }
}

eckit::URI DB::uri() const {
    return catalogue_->uri();
}

void DB::overlayDB(const DB& otherDB, const std::set<std::string>& variableKeys, bool unmount) {
    if (catalogue_->type() == TocEngine::typeName() && otherDB.catalogue_->type() == TocEngine::typeName())  {
        CatalogueWriter* cat = dynamic_cast<CatalogueWriter*>(catalogue_.get());
        ASSERT(cat);

        cat->overlayDB(*(otherDB.catalogue_), variableKeys, unmount);
    }
}

void DB::reconsolidate() {
    CatalogueWriter* cat = dynamic_cast<CatalogueWriter*>(catalogue_.get());
    ASSERT(cat);

    cat->reconsolidate();
}

void DB::index(const Key &key, const eckit::PathName &path, eckit::Offset offset, eckit::Length length) {
    if (catalogue_->type() == TocEngine::typeName()) {
        CatalogueWriter* cat = dynamic_cast<CatalogueWriter*>(catalogue_.get());
        ASSERT(cat);

        cat->index(key, eckit::URI("file", path), offset, length);
    }
}

void DB::dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const {
    catalogue_->dump(out, simple, conf);
}

DbStats DB::stats() const {
    CatalogueReader* cat = dynamic_cast<CatalogueReader*>(catalogue_.get());
    ASSERT(cat);

    return cat->stats();
}

void DB::control(const ControlAction& action, const ControlIdentifiers& identifiers) const {
    catalogue_->control(action, identifiers);
}
bool DB::enabled(const ControlIdentifier& controlIdentifier) const {
    return catalogue_->enabled(controlIdentifier);
};

void DB::print( std::ostream &out ) const {
    catalogue_->print(out);
}

std::ostream &operator<<(std::ostream &s, const DB &x) {
    x.print(s);
    return s;
}

DBVisitor::~DBVisitor() {
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

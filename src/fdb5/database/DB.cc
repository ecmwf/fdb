/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/LibFdb5.h"
#include "fdb5/database/DB.h"
#include "fdb5/database/Field.h"

using eckit::Log;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

std::unique_ptr<DB> DB::buildReader(const Key &key, const fdb5::Config& config) {
    return std::move(std::unique_ptr<DB>(new DB(key, config, true)));
}
std::unique_ptr<DB> DB::buildWriter(const Key &key, const fdb5::Config& config) {
    return std::move(std::unique_ptr<DB>(new DB(key, config, false)));
}
std::unique_ptr<DB> DB::buildReader(const eckit::URI& uri, const fdb5::Config& config) {
    return std::move(std::unique_ptr<DB>(new DB(uri, config, true)));
}
std::unique_ptr<DB> DB::buildWriter(const eckit::URI& uri, const fdb5::Config& config) {
    return std::move(std::unique_ptr<DB>(new DB(uri, config, false)));
}

DB::DB(const Key& key, const fdb5::Config& config, bool read) : buildByKey_(true) {

    catalogue_ = CatalogueFactory::instance().build(key, config, read);
    //store_ = StoreFactory::instance().build(catalogue_->schema(), key, config);
}

DB::DB(const eckit::URI& uri, const fdb5::Config& config, bool read) : buildByKey_(false) {

    catalogue_ = CatalogueFactory::instance().build(uri, config, read);
    //store_ = StoreFactory::instance().build(catalogue_->schema(), uri, config);
}

Store& DB::store() const {
    if (store_ == nullptr) {
        if (buildByKey_)
            store_ = StoreFactory::instance().build(catalogue_->schema(), catalogue_->key(), catalogue_->config());
        else
            store_ = StoreFactory::instance().build(catalogue_->schema(), catalogue_->uri(), catalogue_->config());
    }

    return *store_;
}

std::string DB::dbType() const {
    return catalogue_->type();// + ":" + store_->type();
}

const Key& DB::key() const {
    return catalogue_->key();
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

void DB::axis(const std::string &keyword, eckit::StringSet &s) const {
    catalogue_->axis(keyword, s);
}

eckit::DataHandle *DB::retrieve(const Key& key) const {

    eckit::Log::debug<LibFdb5>() << "Trying to retrieve key " << key << std::endl;

    CatalogueReader* cat = dynamic_cast<CatalogueReader*>(catalogue_.get());
    if (cat != nullptr) {
        Field field;
        Key remapKey;
        if (cat->retrieve(key, field, remapKey)) {
            return store().retrieve(field, remapKey);
        }
    }

    return nullptr;
}

void DB::archive(const Key& key, const void* data, eckit::Length length) {

    CatalogueWriter* cat = dynamic_cast<CatalogueWriter*>(catalogue_.get());

    if (cat != nullptr) {
        const Index& idx = cat->currentIndex();
        cat->archive(key, store().archive(idx.key(), data, length));
    }
}

bool DB::open() {
    bool ret = catalogue_->open();
    if (!ret)
            return ret;
    store().schema(catalogue_->schema());
    return store().open();
}

void DB::flush() {
    if (store_ != nullptr)
        store_->flush();
    catalogue_->flush();
}

void DB::close() {
    flush();
    if (store_ != nullptr)
        store_->close();
    catalogue_->close();
}

bool DB::exists() const {
    return (catalogue_->exists()/* && store_->exists()*/);
}

void DB::hideContents() {
    if (catalogue_->type() == "toc") {
        catalogue_->hideContents();
    }
}

eckit::URI DB::uri() const {
    return catalogue_->uri();
}

void DB::overlayDB(const DB& otherDB, const std::set<std::string>& variableKeys, bool unmount) {
    if (catalogue_->type() == "toc" && otherDB.catalogue_->type() == "toc")  {
        CatalogueWriter* cat = dynamic_cast<CatalogueWriter*>(catalogue_.get());
        if (cat != nullptr)
            cat->overlayDB(*(otherDB.catalogue_), variableKeys, unmount);
    }
}

void DB::reconsolidateIndexesAndTocs() {
    if (catalogue_->type() == "toc") {
        catalogue_->reconsolidateIndexesAndTocs();
    }
}

void DB::dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const {
    catalogue_->dump(out, simple, conf);
}

DbStats DB::stats() const {
    CatalogueReader* cat = dynamic_cast<CatalogueReader*>(catalogue_.get());
    if (cat != nullptr)
        return cat->stats();
}

void DB::control(const ControlAction& action, const ControlIdentifiers& identifiers) const {
    catalogue_->control(action, identifiers);
}

bool DB::retrieveLocked() const {
    return catalogue_->retrieveLocked();
}
bool DB::archiveLocked() const {
    return catalogue_->archiveLocked();
}
bool DB::listLocked() const {
    return catalogue_->listLocked();
}
bool DB::wipeLocked() const {
    return catalogue_->wipeLocked();
}

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

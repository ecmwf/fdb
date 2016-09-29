/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"

#include "fdb5/pmem/PMemDB.h"
#include "fdb5/toc/RootManager.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

PMemDB::PMemDB(const Key& key) :
    DB(key),
    // Utilise the RootManager from the TocDB to get a sensible location. Note that we are NOT
    // using this as a directory, but rather as a pool file.
    pool_(initialisePool(RootManager::directory(key))),
    root_(*pool_->root()),
    currentIndex_(0) {}


PMemDB::PMemDB(const PathName& poolFile) :
    DB(Key()),
    pool_(initialisePool(poolFile)),
    root_(*pool_->root()),
    currentIndex_(0) {}


PMemDB::~PMemDB() {
}


PMemPool* PMemDB::initialisePool(const PathName& poolFile) {

    return PMemPool::obtain(poolFile, Resource<size_t>("fdbPMemPoolSize", 1024 * 1024 * 1024));
}

//----------------------------------------------------------------------------------------------------------------------

bool PMemDB::open() {
    NOTIMP;
}

void PMemDB::archive(const Key &key, const void *data, Length length) {

    ASSERT(currentIndex_ != 0);

    currentIndex_->createDataNode(key, data, length);
    Log::error() << "key: " << key << ", len: " << length << std::endl;
    Log::error() << "Archive not implemented for " << *this << std::endl;
    NOTIMP;
}

eckit::DataHandle * PMemDB::retrieve(const Key &key) const {
    Log::error() << "retrieve not implemented for " << *this << std::endl;
    NOTIMP;
}

void PMemDB::flush() {
    // Intentionally left blank.
    // The libpmemobj functionality works with atomic writes, and everything is flushed before it returns success.
}

void PMemDB::close() {
    NOTIMP;
}

void PMemDB::checkSchema(const Key &key) const {
    Log::error() << "Key: " << key << std::endl;
    Log::error() << "checkSchema not implemented for " << *this << std::endl;
//    NOTIMP;
}

void PMemDB::axis(const std::string &keyword, eckit::StringSet &s) const {
    Log::error() << "axis not implemented for " << *this << std::endl;
    NOTIMP;
}

bool PMemDB::selectIndex(const Key &key) {

    // TODO: What if it isn't there, and we are trying to read?

    currentIndex_ = &root_.getIndex(key);
    return true;
}

void PMemDB::deselectIndex() {

    // This is essentially a NOP, as we don't have any files to open, etc.
    currentIndex_ = 0;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

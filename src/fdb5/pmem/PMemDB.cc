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

#include "fdb5/config/MasterConfig.h"
#include "fdb5/pmem/PMemDB.h"
#include "fdb5/toc/RootManager.h"

using namespace eckit;

namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------

PMemDB::PMemDB(const Key& key) :
    DB(key),
    // Utilise the RootManager from the TocDB to get a sensible location. Note that we are NOT
    // using this as a directory, but rather as a pool file.
    poolDir_(RootManager::directory(key)),
    pool_(initialisePool(poolDir_)),
    root_(*pool_->root()),
    dataPoolMgr_(poolDir_, pool_->root()),
    currentIndex_(0) {}


PMemDB::PMemDB(const PathName& poolDir) :
    DB(Key()),
    poolDir_(poolDir),
    pool_(initialisePool(poolDir)),
    root_(*pool_->root()),
    dataPoolMgr_(poolDir_, pool_->root()),
    currentIndex_(0) {}


PMemDB::~PMemDB() {
    close();
}


Pool* PMemDB::initialisePool(const PathName& poolFile) {

    return Pool::obtain(poolFile, Resource<size_t>("fdbPMemPoolSize", 1024 * 1024 * 1024));
}


//----------------------------------------------------------------------------------------------------------------------

bool PMemDB::open() {
    return true;
}

void PMemDB::archive(const Key &key, const void *data, Length length) {
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

    // Close any open indices

    for (IndexStore::iterator it = indexes_.begin(); it != indexes_.end(); ++it) {
        Index* idx = it->second;
        idx->close();
        delete idx;
    }
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
    NOTIMP;
}

void PMemDB::deselectIndex() {

    // This is essentially a NOP, as we don't have any files to open, etc.
    currentIndex_ = 0;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

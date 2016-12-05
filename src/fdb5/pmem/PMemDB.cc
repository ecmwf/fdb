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
#include "eckit/log/Timer.h"

#include "fdb5/config/MasterConfig.h"
#include "fdb5/pmem/PMemDB.h"
#include "fdb5/toc/RootManager.h"
#include "fdb5/rules/Rule.h"

#include "pmem/PersistentString.h"

using namespace eckit;
using namespace pmem;

namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------

PMemDB::PMemDB(const Key& key) :
    DB(key),
    // Utilise the RootManager from the TocDB to get a sensible location. Note that we are NOT
    // using this as a directory, but rather as a pool file.
    poolDir_(RootManager::directory(key)),
    currentIndex_(0) {}
//}


PMemDB::PMemDB(const PathName& poolDir) :
    DB(Key()),
    poolDir_(poolDir),
    currentIndex_(0) {}

PMemDB::~PMemDB() {
    close();
}


void PMemDB::initialisePool() {

    ASSERT(currentIndex_ == 0);

    // Get (or create) the pool
    pool_.reset(Pool::obtain(poolDir_, Resource<size_t>("fdbPMemPoolSize", 1024 * 1024 * 1024)));

    root_ = &pool_->root();

    dataPoolMgr_.reset(new DataPoolManager(poolDir_, *root_, pool_->baseRoot().uuid()));

    // Initialise the schema object for comparison against the global schema.

    const PersistentString& schemaBuf(root_->schema());
    std::string s(schemaBuf.c_str(), schemaBuf.length());
    std::istringstream iss(s);
    schema_.load(iss);
}


//----------------------------------------------------------------------------------------------------------------------

bool PMemDB::open() {

    if (!pool_)
        initialisePool();

    return true;
}

bool PMemDB::exists() const {
    return Pool::exists(poolDir_);
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
    eckit::Timer timer("PMemDB::checkSchema()");
    ASSERT(key.rule());
    schema_.compareTo(key.rule()->schema());
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

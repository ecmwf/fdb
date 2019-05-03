/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"
#include "eckit/log/Timer.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Key.h"
#include "fdb5/rules/Rule.h"

#include "fdb5/pmem/PMemDB.h"
#include "fdb5/pmem/PMemFieldLocation.h"
#include "fdb5/pmem/PoolManager.h"
#include "fdb5/pmem/PMemStats.h"

#include "pmem/PersistentString.h"

#include <pwd.h>
#include <unistd.h>
#include <sys/types.h>

using namespace eckit;
using namespace pmem;

namespace fdb5 {
namespace pmem {

static std::string userName(long id) {

  struct passwd *p = getpwuid(id);

  if (p) {
    return p->pw_name;
  } else {
    return eckit::Translator<long, std::string>()(id);
  }
}

//----------------------------------------------------------------------------------------------------------------------

PMemDB::PMemDB(const Key& key, const Config& config) :
    DB(key),
    poolDir_(PoolManager(config).pool(key)),
    dbConfig_(config),
    init_(false)
{

    // If opened with a key in this manner, it is for write, so should open up fully.
    initialisePool();
}


PMemDB::PMemDB(const PathName& poolDir, const Config& config) :
    DB(Key()),
    poolDir_(poolDir),
    dbConfig_(config),
    init_(false)
{
    initialisePool();
}

PMemDB::~PMemDB() {
    close();
}

void PMemDB::initialisePool() {

    ASSERT(init_ == false);

    // Get (or create) the pool
    pool_.reset(Pool::obtain(poolDir_, Resource<size_t>("fdbPMemPoolSize", 1024 * 1024 * 1024), dbKey_, dbConfig_.schemaPath()));

    root_ = &pool_->root();

    dataPoolMgr_.reset(new DataPoolManager(poolDir_, *root_, pool_->baseRoot().uuid()));

    // Initialise the schema object for comparison against the global schema.

    const PersistentString& schemaBuf(root_->schema());
    std::string s(schemaBuf.c_str(), schemaBuf.length());
    std::istringstream iss(s);
    schema_.load(iss);

    if (dbKey_.empty())
        dbKey_ = root_->databaseKey();
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
    Log::error() << "archive not implemented for " << *this << std::endl;
    NOTIMP;
}

void PMemDB::visitEntries(EntryVisitor& visitor, bool sorted) {

    // There is no meaningfully more-efficient way or sorting the indexes!
    (void) sorted;

    Log::debug<LibFdb5>() << "Visiting entries in DB with key " << dbKey_ << std::endl;

    visitor.visitDatabase(*this);

    if (visitor.visitIndexes()) {
        ASSERT(pool_ && root_);
        root_->visitLeaves(visitor, *dataPoolMgr_, schema());
    }

    visitor.databaseComplete(*this);
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
        Index& idx = it->second;
        idx.close();
    }
    indexes_.clear();
}

void PMemDB::axis(const std::string &keyword, eckit::StringSet &s) const {
    Log::error() << "axis not implemented for " << *this << std::endl;
    NOTIMP;
}

bool PMemDB::selectIndex(const Key &key) {
    NOTIMP;
}

void PMemDB::dump(std::ostream& out, bool simple) const {

    // Check that things are open
    ASSERT(pool_);

    // Output details of the DB itself

    out << std::endl << "PMEM_DB" << std::endl;
    out << "  Version: " << root_->version() << std::endl;
    out << "  Created: " << root_->created() << std::endl;

    out << "  uid: ";
    struct passwd* p = ::getpwuid(root_->uid());
    if (p)
        out << p->pw_name;
    else
        out << root_->uid();
    out << std::endl;

    Log::info() << "  Key: " << dbKey_ << std::endl << std::endl;

    // And dump the rest of the stuff

    struct DumpVisitor : EntryVisitor {
        DumpVisitor(std::ostream& out) : out_(out) {}
        void visitDatum(const Field& field, const Key& key) override {
            out_ << "ENTRY" << std::endl;
            out_ << "  " << key << std::endl;
            field.location().dump(out_);
        }
        std::ostream& out_;
    };

    DumpVisitor visitor(out);
    const_cast<PMemDB*>(this)->visitEntries(visitor);
}

std::string PMemDB::owner() const {

    ASSERT(pool_);
    ASSERT(root_);

    return userName(root_->uid());
}

void PMemDB::visit(DBVisitor &visitor) {
    visitor(*this);
}

const Schema& PMemDB::schema() const {

    // We must have opened the DB for the schema to have been loaded.
    ASSERT(pool_);
    return schema_;
}

void PMemDB::deselectIndex() {
    currentIndex_ = Index(); // essentially a no-op, as we don't have any files to open, etc.
}

DbStats PMemDB::statistics() const
{
    PMemDbStats* stats = new PMemDbStats();

    stats->dataPoolsCount_ += dataPoolsCount();
    stats->indexPoolsCount_ += 1; // FTM we use a single pool, but we may support overspill in the future
    stats->indexesCount_ += 1;    // FTM we use a single index, but we may support overspill in the future

    stats->schemaSize_ += schemaSize();
    stats->indexPoolsSize_ += pool_->size();
    stats->dataPoolsSize_  += dataPoolsSize();

    return DbStats(stats);
}

StatsReportVisitor* PMemDB::statsReportVisitor() const {
    return new PMemStatsReportVisitor(*this);
}

PurgeVisitor *PMemDB::purgeVisitor() const {
    NOTIMP;
    //    return new TocPurgeVisitor(*this);
}

void PMemDB::maskIndexEntry(const Index &index) const {
    NOTIMP;
}


std::vector<Index> PMemDB::indexes(bool sorted) const {
    throw eckit::NotImplemented("TocDB::indexes() isn't implemented for this DB type "
                                "-- perhaps this is a writer?", Here());
}

eckit::PathName PMemDB::basePath() const {
    return poolDir_;
}

std::vector<PathName> PMemDB::metadataPaths() const {

    ASSERT(pool_);
    ASSERT(root_);
    ASSERT(dataPoolMgr_);

    return { pool_->path() };
}


size_t PMemDB::dataPoolsCount() const {
    return dataPoolMgr_->dataPoolsCount();
}

size_t PMemDB::dataPoolsSize() const {

    return dataPoolMgr_->dataSize();
}

std::string PMemDB::dbType() const
{
    return PMemDB::dbTypeName();
}

void PMemDB::checkUID() const {

    ASSERT(pool_);
    ASSERT(root_);

    static bool fdbOnlyCreatorCanWrite = eckit::Resource<bool>("fdbOnlyCreatorCanWrite", true);
    if (!fdbOnlyCreatorCanWrite) {
        return;
    }

    static std::vector<std::string> fdbSuperUsers = eckit::Resource<std::vector<std::string> >("fdbSuperUsers", "", true);

    long uid = ::getuid();

    if (root_->uid() != uid) {

        if(std::find(fdbSuperUsers.begin(), fdbSuperUsers.end(), userName(uid)) == fdbSuperUsers.end()) {

            std::ostringstream oss;
            oss << "Only user '"
                << userName(root_->uid())
                << "' can write to FDB "
                << basePath()
                << ", current user is '"
                << userName(uid)
                << "'";

            throw eckit::UserError(oss.str());
        }
    }
}

size_t PMemDB::schemaSize() const {
    ASSERT(pool_);
    return root_->schema().length();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

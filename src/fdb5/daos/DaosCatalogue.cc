/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/io/Buffer.h"
#include "eckit/config/Resource.h"
// #include "eckit/log/Timer.h"

#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/LibFdb5.h"
// #include "fdb5/rules/Rule.h"
// #include "fdb5/toc/RootManager.h"
// #include "fdb5/toc/TocPurgeVisitor.h"
// #include "fdb5/toc/TocStats.h"
// #include "fdb5/toc/TocWipeVisitor.h"
// #include "fdb5/toc/TocMoveVisitor.h"

#include "fdb5/daos/DaosCatalogue.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"

// using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosCatalogue::DaosCatalogue(const Key& key, const fdb5::Config& config) :
    Catalogue(key, ControlIdentifiers{}, config) {

    // TODO: apply the mechanism in RootManager::directory, using
    //   FileSpaceTables to determine root_pool_name_ according to key
    //   and using DbPathNamerTables to determine db_cont_name_ according
    //   to key

    pool_ = "default";

    eckit::LocalConfiguration c{};

    if (config_.has("daos")) c = config_.getSubConfiguration("daos");
    if (c.has("catalogue")) pool_ = c.getSubConfiguration("catalogue").getString("pool", pool_);

    pool_ = eckit::Resource<std::string>("fdbDaosCataloguePool;$FDB_DAOS_CATALOGUE_POOL", pool_);

    db_cont_ = key.valuesToString();

    if (c.has("client"))
        fdb5::DaosManager::instance().configure(c.getSubConfiguration("client"));

}

// TocCatalogue::TocCatalogue(const Key& key, const TocPath& tocPath, const fdb5::Config& config) :
//     Catalogue(key, tocPath.controlIdentifiers_, config),
//     TocHandler(tocPath.directory_, config) {}

DaosCatalogue::DaosCatalogue(const eckit::URI& uri, const ControlIdentifiers& controlIdentifiers, const fdb5::Config& config) :
    Catalogue(Key(), controlIdentifiers, config) {

    //// Read the real DB key into the DB base object
    //dbKey_ = databaseKey();

    fdb5::DaosName n{uri};
    ASSERT(n.hasOID());

    pool_ = n.poolName();
    
    db_cont_ = n.contName();

    eckit::LocalConfiguration c{};
    if (c.has("daos")) c = c.getSubConfiguration("daos");
    if (c.has("client"))
        fdb5::DaosManager::instance().configure(c.getSubConfiguration("client"));

}

// bool TocCatalogue::exists() const {
//     return TocHandler::exists();
// }

// const std::string TocCatalogue::DUMP_PARAM_WALKSUBTOC = "walk";

// void TocCatalogue::dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const {
//     bool walkSubToc = false;
//     conf.get(DUMP_PARAM_WALKSUBTOC, walkSubToc);

//     TocHandler::dump(out, simple, walkSubToc);
// }

// eckit::URI TocCatalogue::uri() const {
//     return eckit::URI(TocEngine::typeName(), basePath());
// }

// const Schema& TocCatalogue::schema() const {
//     return schema_;
// }

// const eckit::PathName& TocCatalogue::basePath() const {
//     return directory_;
// }

// std::vector<PathName> TocCatalogue::metadataPaths() const {

//     std::vector<PathName> paths(subTocPaths());

//     paths.emplace_back(schemaPath());
//     paths.emplace_back(tocPath());

//     std::vector<PathName>&& lpaths(lockfilePaths());
//     paths.insert(paths.end(), lpaths.begin(), lpaths.end());

//     return paths;
// }

// void TocCatalogue::visitEntries(EntryVisitor& visitor, const Store& store, bool sorted) {

//     std::vector<Index> all = indexes(sorted);

//     // Allow the visitor to selectively reject this DB.
//     if (visitor.visitDatabase(*this, store)) {
//         if (visitor.visitIndexes()) {
//             for (const Index& idx : all) {
//                 if (visitor.visitEntries()) {
//                     idx.entries(visitor); // contains visitIndex
//                 } else {
//                     visitor.visitIndex(idx);
//                 }
//             }
//         }

//         visitor.catalogueComplete(*this);
//     }

// }

void DaosCatalogue::loadSchema() {

    eckit::Timer timer("DaosCatalogue::loadSchema()", eckit::Log::debug<fdb5::LibFdb5>());

    fdb5::DaosKeyValueName n{pool_, db_cont_, main_kv_};
    std::unique_ptr<eckit::DataHandle> h(n.dataHandle("schema"));
    h->openForRead();
    eckit::AutoClose closer(*h);
    eckit::Buffer buff{(size_t) h->size()};
    h->read(buff, h->size());
    std::istringstream stream{std::string(buff)};
    schema_.load(stream);

}

// StatsReportVisitor* TocCatalogue::statsReportVisitor() const {
//     return new TocStatsReportVisitor(*this);
// }

// PurgeVisitor *TocCatalogue::purgeVisitor(const Store& store) const {
//     return new TocPurgeVisitor(*this, store);
// }

// WipeVisitor* TocCatalogue::wipeVisitor(const Store& store, const metkit::mars::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const {
//     return new TocWipeVisitor(*this, store, request, out, doit, porcelain, unsafeWipeAll);
// }

// MoveVisitor* TocCatalogue::moveVisitor(const Store& store, const metkit::mars::MarsRequest& request, const eckit::URI& dest, bool removeSrc, int removeDelay, int threads) const {
//     return new TocMoveVisitor(*this, store, request, dest, removeSrc, removeDelay, threads);
// }

// void TocCatalogue::maskIndexEntry(const Index &index) const {
//     TocHandler handler(basePath(), config_);
//     handler.writeClearRecord(index);
// }

// std::vector<Index> TocCatalogue::indexes(bool sorted) const {
//     return loadIndexes(sorted);
// }

// void TocCatalogue::allMasked(std::set<std::pair<URI, Offset>>& metadata,
//                       std::set<URI>& data) const {
//     enumerateMasked(metadata, data);
// }

// std::string TocCatalogue::type() const
// {
//     return TocCatalogue::catalogueTypeName();
// }

// void TocCatalogue::checkUID() const {
//     TocHandler::checkUID();
// }

// void TocCatalogue::remove(const eckit::PathName& path, std::ostream& logAlways, std::ostream& logVerbose, bool doit) {
//     if (path.isDir()) {
//         logVerbose << "rmdir: ";
//         logAlways << path << std::endl;
//         if (doit) path.rmdir(false);
//     } else {
//         logVerbose << "Unlinking: ";
//         logAlways << path << std::endl;
//         if (doit) path.unlink(false);
//     }
// }

// void TocCatalogue::control(const ControlAction& action, const ControlIdentifiers& identifiers) const {
//     TocHandler::control(action, identifiers);
// }

// bool TocCatalogue::enabled(const ControlIdentifier& controlIdentifier) const {
//     return Catalogue::enabled(controlIdentifier) && TocHandler::enabled(controlIdentifier);
// }

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

// #include <dirent.h>

// #include <algorithm>
// #include <cstring>
// #include <list>
// #include <ostream>

// #include "eckit/eckit.h"

// #include "eckit/filesystem/LocalFileManager.h"
// #include "eckit/filesystem/LocalPathName.h"
// #include "eckit/filesystem/StdDir.h"
// #include "eckit/log/Log.h"
// #include "eckit/os/BackTrace.h"
// #include "eckit/os/Stat.h"
// #include "eckit/utils/Regex.h"

#include "fdb5/LibFdb5.h"
// #include "fdb5/rules/Schema.h"
// #include "fdb5/toc/RootManager.h"
#include "fdb5/daos/DaosEngine.h"
// #include "fdb5/toc/TocHandler.h"
#include "eckit/serialisation/MemoryStream.h"

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosName.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

void DaosEngine::configureDaos(const Config& config) const {

    if (daos_config_.has_value()) return;

    daos_config_.emplace(eckit::LocalConfiguration());
    if (config.has("daos")) daos_config_.emplace(config.getSubConfiguration("daos"));
    if (daos_config_->has("client"))
        fdb5::DaosManager::instance().configure(daos_config_->getSubConfiguration("client"));

}

// void TocEngine::scan_dbs(const std::string& path, std::list<std::string>& dbs) const {

//     if ((eckit::PathName(path) / "toc").exists()) {
//         dbs.push_back(path);
//         return;
//     }

//     eckit::StdDir d(path.c_str());
//     if (d == nullptr) {
//         // If fdb-wipe is running in parallel, it is perfectly legit for a (non-matching)
//         // path to have disappeared
//         if (errno == ENOENT) {
//             return;
//         }

//         // It should not be an error if we don't have permission to read a path/DB in the
//         // tree. This is a multi-user system.
//         if (errno == EACCES) {
//             return;
//         }

//         Log::error() << "opendir(" << path << ")" << Log::syserr << std::endl;
//         throw FailedSystemCall("opendir");
//     }

//     // Once readdir_r finally gets deprecated and removed, we may need to 
//     // protecting readdir() as not yet guarranteed thread-safe by POSIX
//     // technically it should only be needed on a per-directory basis
//     // this should be a resursive mutex
//     // AutoLock<Mutex> lock(mutex_); 

//     for(;;)
//     {
//         struct dirent* e = d.dirent();
//         if (e == nullptr) {
//             break;
//         }

//         if(e->d_name[0] == '.') {
//             if(e->d_name[1] == 0 || (e->d_name[1] =='.' && e->d_name[2] == 0))
//                 continue;
//         }

//         std::string full = path;
//         if (path[path.length()-1] != '/') full += "/";
//         full += e->d_name;

//         bool do_stat = true;

// #if defined(eckit_HAVE_DIRENT_D_TYPE)
//         do_stat = false;
//         if (e->d_type == DT_DIR) {
//             scan_dbs(full.c_str(), dbs);
//         } else if (e->d_type == DT_UNKNOWN) {
//             do_stat = true;
//         }
// #endif
//         if(do_stat) {
//             eckit::Stat::Struct info;
//             if(eckit::Stat::stat(full.c_str(), &info) == 0)
//             {
//                 if(S_ISDIR(info.st_mode)) {
//                     scan_dbs(full.c_str(), dbs);
//                 }
//             }
//             else Log::error() << "Cannot stat " << full << Log::syserr << std::endl;
//         }
//     }
// }

std::string DaosEngine::name() const {
    return DaosEngine::typeName();
}

// std::string TocEngine::dbType() const {
//     return TocEngine::typeName();
// }

// eckit::URI TocEngine::location(const Key& key, const Config& config) const
// {
//     return URI("toc", CatalogueRootManager(config).directory(key).directory_);
// }

bool DaosEngine::canHandle(const eckit::URI& uri, const Config& config) const {

    configureDaos(config);

    if (uri.scheme() != "daos")
        return false;

    fdb5::DaosName n{uri};

    if (!n.hasOID()) return false;

    /// @todo: check contName is not root_cont_. root_cont_ should be populated in
    ///   configureDaos as done in DaosCommon
    // bool is_root_name = (n.contName().find(root_cont_) != std::string::npos);
    bool is_root_name = false;
    bool is_store_name = (n.contName().find("_") != std::string::npos);
    fdb5::DaosName n2{n.poolName(), n.contName(), catalogue_kv_};
    n2.generateOID();
    bool is_catalogue_kv = (!is_root_name && !is_store_name && n.OID() == n2.OID());

    return is_catalogue_kv && n.exists();

}

std::unique_ptr<DB> DaosEngine::buildReader(const eckit::URI& uri, const Config& config) const {

    Log::debug<LibFdb5>() << "FDB processing DAOS DB at KV " << uri << std::endl;

    return DB::buildReader(uri, config);

}

bool DaosEngine::toExistingDBURI(eckit::URI& uri, const Config& config) const {

    configureDaos(config);

    fdb5::DaosName n{uri};
    if (!n.hasContName()) return false;

    fdb5::DaosName n2{n.poolName(), n.contName(), catalogue_kv_};

    if (!n2.exists()) return false;

    uri = n2.URI();
    return true;

}

// static void matchKeyToDB(const Key& key, std::set<Key>& keys, const char* missing, const Config& config)
// {
//     const Schema& schema = config.schema();
//     schema.matchFirstLevel(key, keys, missing);
// }

// static void matchRequestToDB(const metkit::mars::MarsRequest& rq, std::set<Key>& keys, const char* missing, const Config& config)
// {
//     const Schema& schema = config.schema();
//     schema.matchFirstLevel(rq, keys, missing);
// }

// static constexpr const char* regexForMissingValues = "[^:/]*";

// std::set<eckit::PathName> TocEngine::databases(const std::set<Key>& keys,
//                                                const std::vector<eckit::PathName>& roots,
//                                                const Config& config) const {

//     std::set<eckit::PathName> result;

//     for (std::vector<eckit::PathName>::const_iterator j = roots.begin(); j != roots.end(); ++j) {

//         Log::debug<LibFdb5>() << "Scanning for TOC FDBs in root " << *j << std::endl;

//         std::list<std::string> dbs;
//         scan_dbs(*j, dbs);

//         for (std::set<Key>::const_iterator i = keys.begin(); i != keys.end(); ++i) {

//             std::vector<std::string> dbpaths = CatalogueRootManager(config).possibleDbPathNames(*i, regexForMissingValues);

//             for(std::vector<std::string>::const_iterator dbpath = dbpaths.begin(); dbpath != dbpaths.end(); ++dbpath) {

//                 Regex re("^" + *j + "/" + *dbpath + "$");

//                 Log::debug<LibFdb5>() << " -> key i " << *i
//                                      << " dbpath " << *dbpath
//                                      << " pathregex " << re << std::endl;

//                 for (std::list<std::string>::const_iterator k = dbs.begin(); k != dbs.end(); ++k) {

//                     Log::debug<LibFdb5>() << "    -> db " << *k << std::endl;

//                     if(result.find(*k) != result.end()) {
//                         continue;
//                     }

//                     if (re.match(*k)) {
//                         result.insert(*k);
//                     }
//                 }
//             }
//         }
//     }

//     Log::debug<LibFdb5>() << "TocEngine::databases() results " << result << std::endl;

//     return result;
// }

// std::vector<eckit::URI> TocEngine::databases(const Key& key,
//                                                   const std::vector<eckit::PathName>& roots,
//                                                   const Config& config) const {

//     std::set<Key> keys;

//     matchKeyToDB(key, keys, regexForMissingValues, config);

//     Log::debug<LibFdb5>() << "Matched DB schemas for key " << key << " -> keys " << keys << std::endl;

//     std::set<eckit::PathName> databasesMatchRegex(databases(keys, roots, config));

//     std::vector<eckit::URI> result;
//     for (const auto& path : databasesMatchRegex) {
//         try {
//             TocHandler toc(path, config);
//             if (toc.databaseKey().match(key)) {
//                 Log::debug<LibFdb5>() << " found match with " << path << std::endl;
//                 result.push_back(eckit::URI("toc", path));
//             }
//         } catch (eckit::Exception& e) {
//             eckit::Log::error() <<  "Error loading FDB database from " << path << std::endl;
//             eckit::Log::error() << e.what() << std::endl;
//         }
//     }

//     return result;
// }

// std::vector<eckit::URI> TocEngine::databases(const metkit::mars::MarsRequest& request,
//                                                   const std::vector<eckit::PathName>& roots,
//                                                   const Config& config) const {

//     std::set<Key> keys;

// //    matchRequestToDB(request, keys, regexForMissingValues, config);
//     matchRequestToDB(request, keys, "", config);

//     Log::debug<LibFdb5>() << "Matched DB schemas for request " << request << " -> keys " << keys << std::endl;

//     std::set<eckit::PathName> databasesMatchRegex(databases(keys, roots, config));

//     std::vector<eckit::URI> result;
//     for (const auto& path : databasesMatchRegex) {
//         try {
//             TocHandler toc(path, config);
//             if (toc.databaseKey().partialMatch(request)) {
//                 Log::debug<LibFdb5>() << " found match with " << path << std::endl;
//                 result.push_back(eckit::URI("toc", path));
//             }
//         } catch (eckit::Exception& e) {
//             eckit::Log::error() <<  "Error loading FDB database from " << path << std::endl;
//             eckit::Log::error() << e.what() << std::endl;
//         }
//     }

//     return result;
// }

// std::vector<eckit::URI> TocEngine::allLocations(const Key& key, const Config& config) const
// {
//     return databases(key, CatalogueRootManager(config).allRoots(key), config);
// }

std::vector<eckit::URI> DaosEngine::visitableLocations(const Key& key, const Config& config) const
{

    // return databases(key, CatalogueRootManager(config).visitableRoots(key), config);

    /// ---

    /// @note: code mostly copied from DaosCommon
    /// @note: should rather use DaosCommon, but can't inherit from it here as DaosEngine is 
    ///   always instantiated even if daos is not used, and then DaosCommon would be unnecessarily 
    ///   initialised. If owning a private instance of DaosCommon here, then the private members of 
    ///   DaosCommon are not accessible from here

    configureDaos(config);

    std::string pool = "default";
    std::string root_cont = "root";

    if (daos_config_->has("catalogue")) pool = daos_config_->getSubConfiguration("catalogue").getString("pool", pool);
    if (daos_config_->has("catalogue")) root_cont = daos_config_->getSubConfiguration("catalogue").getString("root_cont", root_cont);

    pool = eckit::Resource<std::string>("fdbDaosCataloguePool;$FDB_DAOS_CATALOGUE_POOL", pool);
    root_cont = eckit::Resource<std::string>("fdbDaosCatalogueRootCont;$FDB_DAOS_CATALOGUE_ROOT_CONT", root_cont);

    fdb5::DaosOID main_kv_oid{0, 0, DAOS_OT_KV_HASHED, OC_S1};  /// @todo: take oclass from config

    /// ---

    fdb5::DaosKeyValueName main_kv_name{pool, root_cont, main_kv_oid};

    std::vector<eckit::URI> res{};
    if (!main_kv_name.exists()) return res;

    fdb5::DaosSession s{};
    fdb5::DaosKeyValue main_kv{s, main_kv_name};

    for (const auto& k : main_kv.keys()) {

        try {

            daos_size_t size = main_kv.size(k);
            std::vector<char> v(size);
            main_kv.get(k, v.data(), size);
            eckit::URI uri(std::string(v.begin(), v.end()));
            ASSERT(uri.scheme() == typeName());

            /// @todo: this exact deserialisation is performed twice. Once here and once 
            ///   in DaosCatalogue::(uri, ...). Try to avoid one.
            fdb5::DaosKeyValueName db_kv_name{uri};
            ASSERT(db_kv_name.exists() && db_kv_name.has("key"));
            fdb5::DaosKeyValue db_kv{s, db_kv_name};
            size = db_kv.size("key");
            std::vector<char> dbkey_data((long) size);
            db_kv.get("key", &dbkey_data[0], size);
            eckit::MemoryStream ms{&dbkey_data[0], size};
            fdb5::Key db_key(ms);

            if (db_key.match(key)) {

                Log::debug<LibFdb5>() << " found match with " << main_kv_name.URI() << " at key " << k << std::endl;
                res.push_back(uri);

            }
        
        } catch (eckit::Exception& e) {
            eckit::Log::error() <<  "Error loading FDB database " << k << " from " << main_kv_name.URI() << std::endl;
            eckit::Log::error() << e.what() << std::endl;
        }

    }

    return res;

}

std::vector<URI> DaosEngine::visitableLocations(const metkit::mars::MarsRequest& request, const Config& config) const
{

    // return databases(request, CatalogueRootManager(config).visitableRoots(request), config);

    /// ---

    /// @note: code mostly copied from DaosCommon
    /// @note: should rather use DaosCommon, but can't inherit from it here as DaosEngine is 
    ///   always instantiated even if daos is not used, and then DaosCommon would be unnecessarily 
    ///   initialised. If owning a private instance of DaosCommon here, then the private members of 
    ///   DaosCommon are not accessible from here

    configureDaos(config);

    std::string pool = "default";
    std::string root_cont = "root";

    if (daos_config_->has("catalogue")) pool = daos_config_->getSubConfiguration("catalogue").getString("pool", pool);
    if (daos_config_->has("catalogue")) root_cont = daos_config_->getSubConfiguration("catalogue").getString("root_cont", root_cont);

    pool = eckit::Resource<std::string>("fdbDaosCataloguePool;$FDB_DAOS_CATALOGUE_POOL", pool);
    root_cont = eckit::Resource<std::string>("fdbDaosCatalogueRootCont;$FDB_DAOS_CATALOGUE_ROOT_CONT", root_cont);

    fdb5::DaosOID main_kv_oid{0, 0, DAOS_OT_KV_HASHED, OC_S1};  /// @todo: take oclass from config

    /// ---

    fdb5::DaosKeyValueName main_kv_name{pool, root_cont, main_kv_oid};

    std::vector<eckit::URI> res;

    if (!main_kv_name.exists()) return res;

    fdb5::DaosSession s{};
    fdb5::DaosKeyValue main_kv{s, main_kv_name};

    for (const auto& k : main_kv.keys()) {

        try {

            daos_size_t size = main_kv.size(k);
            std::vector<char> v(size);
            main_kv.get(k, v.data(), size);
            eckit::URI uri(std::string(v.begin(), v.end()));
            ASSERT(uri.scheme() == typeName());


            /// @todo: this exact deserialisation is performed twice. Once here and once 
            ///   in DaosCatalogue::(uri, ...). Try to avoid one.
            fdb5::DaosKeyValueName db_kv_name{uri};
            fdb5::DaosKeyValue db_kv{s, db_kv_name};
            size = db_kv.size("key");
            std::vector<char> dbkey_data((long) size);
            db_kv.get("key", &dbkey_data[0], size);
            eckit::MemoryStream ms{&dbkey_data[0], size};
            fdb5::Key db_key(ms);

            if (db_key.partialMatch(request)) {

                Log::debug<LibFdb5>() << " found match with " << main_kv_name.URI() << " at key " << k << std::endl;
                res.push_back(uri);

            }
        
        } catch (eckit::Exception& e) {
            eckit::Log::error() <<  "Error loading FDB database from " << main_kv_name.URI() << std::endl;
            eckit::Log::error() << e.what() << std::endl;
        }

    }

    return res;

}

// std::vector<eckit::URI> TocEngine::writableLocations(const Key& key, const Config& config) const
// {
//     return databases(key, CatalogueRootManager(config).canArchiveRoots(key), config);
// }

// void TocEngine::print(std::ostream& out) const
// {
//     out << "TocEngine()";
// }

static EngineBuilder<DaosEngine> daos_builder;

// static eckit::LocalFileManager manager_toc("toc");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

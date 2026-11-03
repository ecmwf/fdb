// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include <algorithm>

#include "fdb5/daos/DaosCommon.h"

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"

#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosSession.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

DaosCommon::DaosCommon(const fdb5::Config& config, const std::string& component, const fdb5::Key& key) {

    std::vector<std::string> valid{"catalogue", "store"};
    ASSERT(std::find(valid.begin(), valid.end(), component) != valid.end());

    db_cont_ = key.valuesToString();

    readConfig(config, component, true);
}

DaosCommon::DaosCommon(const fdb5::Config& config, const std::string& component, const eckit::URI& uri) {

    /// @note: validity of input URI is not checked here because this constructor is only triggered
    ///   by DB::buildReader in EntryVisitMechanism, where validity of URIs is ensured beforehand

    fdb5::DaosName db_name{uri};
    pool_ = db_name.poolName();
    db_cont_ = db_name.containerName();

    readConfig(config, component, false);
}

void DaosCommon::readConfig(const fdb5::Config& config, const std::string& component, bool readPool) {

    if (readPool) {
        pool_ = "default";
    }
    root_cont_ = "root";

    eckit::LocalConfiguration c{};

    if (config.has("daos")) {
        c = config.getSubConfiguration("daos");
    }
    if (readPool) {
        if (c.has(component)) {
            pool_ = c.getSubConfiguration(component).getString("pool", pool_);
        }
    }
    if (c.has(component)) {
        root_cont_ = c.getSubConfiguration(component).getString("root_cont", root_cont_);
    }

    std::string first_cap{component};
    first_cap[0] = toupper(component[0]);

    std::string all_caps{component};
    for (auto& c : all_caps) {
        c = toupper(c);
    }

    if (readPool) {
        pool_ = eckit::Resource<std::string>("fdbDaos" + first_cap + "Pool;$FDB_DAOS_" + all_caps + "_POOL", pool_);
    }
    root_cont_ = eckit::Resource<std::string>("fdbDaos" + first_cap + "RootCont;$FDB_DAOS_" + all_caps + "_ROOT_CONT",
                                              root_cont_);

    root_kv_.emplace(fdb5::DaosKeyValueName{pool_, root_cont_, main_kv_});
    db_kv_.emplace(fdb5::DaosKeyValueName{pool_, db_cont_, catalogue_kv_});

    if (c.has("client")) {
        fdb5::DaosManager::instance().configure(c.getSubConfiguration("client"));
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

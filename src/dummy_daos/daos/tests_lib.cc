/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * @file   tests_lib.cc
 * @author Nicolau Manubens
 * @date   Jun 2022
 */

#include "eckit/filesystem/TmpDir.h"
#include "eckit/exception/Exceptions.h"

#include "tests_lib.h"
#include "../dummy_daos.h"

static void deldir(eckit::PathName& p) {
    if (!p.exists()) {
        return;
    }

    std::vector<eckit::PathName> files;
    std::vector<eckit::PathName> dirs;
    p.children(files, dirs);

    for (auto& f : files) {
        f.unlink();
    }
    for (auto& d : dirs) {
        deldir(d);
    }

    p.rmdir();
};

extern "C" {

int dmg_pool_create(const char *dmg_config_file,
                    uid_t uid, gid_t gid, const char *grp,
                    const d_rank_list_t *tgts,
                    daos_size_t scm_size, daos_size_t nvme_size,
                    daos_prop_t *prop,
                    d_rank_list_t *svc, uuid_t uuid) {

    std::string random_name = eckit::TmpDir().baseName().path();
    random_name += "_" + std::to_string(getpid());
    const char *random_name_cstr = random_name.c_str();

    uuid_t seed = {0};

    uuid_generate_md5(uuid, seed, random_name_cstr, strlen(random_name_cstr));

    char random_uuid_cstr[37] = "";
    uuid_unparse(uuid, random_uuid_cstr);

    (dummy_daos_root() / random_uuid_cstr).mkdir();

    return 0;

}

int dmg_pool_destroy(const char *dmg_config_file,
                     const uuid_t uuid, const char *grp, int force) {
    
    char uuid_str[37] = "";

    uuid_unparse(uuid, uuid_str);

    eckit::PathName pool_path = dummy_daos_root() / uuid_str;

    deldir(pool_path);

    return 0;

}

}  // extern "C"
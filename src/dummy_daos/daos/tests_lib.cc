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

#include <unistd.h>
#include <cstring>

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/TmpDir.h"
#include "eckit/log/TimeStamp.h"
#include "eckit/runtime/Main.h"
#include "eckit/testing/Filesystem.h"
#include "eckit/utils/MD5.h"

#include "../dummy_daos.h"
#include "tests_lib.h"

//----------------------------------------------------------------------------------------------------------------------

extern "C" {

daos_prop_t* daos_prop_alloc(uint32_t entries_nr) {

    daos_prop_t* prop;

    if (entries_nr > DAOS_PROP_ENTRIES_MAX_NR) {
        eckit::Log::error() << "Too many property entries requested.";
        return NULL;
    }

    D_ALLOC_PTR(prop);
    if (prop == NULL)
        return NULL;

    if (entries_nr > 0) {
        D_ALLOC_ARRAY(prop->dpp_entries, entries_nr);
        if (prop->dpp_entries == NULL) {
            D_FREE(prop);
            return NULL;
        }
    }
    prop->dpp_nr = entries_nr;
    return prop;
}

void daos_prop_fini(daos_prop_t* prop) {
    int i;

    if (!prop->dpp_entries)
        goto out;

    for (i = 0; i < prop->dpp_nr; i++) {
        struct daos_prop_entry* entry;

        entry = &prop->dpp_entries[i];
        if (entry->dpe_type != DAOS_PROP_PO_LABEL)
            NOTIMP;
        D_FREE(entry->dpe_str);
    }

    D_FREE(prop->dpp_entries);
out:
    prop->dpp_nr = 0;
}

void daos_prop_free(daos_prop_t* prop) {
    if (prop == NULL)
        return;

    daos_prop_fini(prop);
    D_FREE(prop);
}

/// @note: pools are implemented as directories. Upon creation, a new random and unique string
///        is generated, a pool UUID is generated from that string, and a directory is created
///        with the UUID as directory name. If a label is specified, a symlink is created with
///        the label as origin file name and the UUID directory as destination. If no label is
///        specified, no symlink is created.

int dmg_pool_create(const char* dmg_config_file, uid_t uid, gid_t gid, const char* grp, const d_rank_list_t* tgts,
                    daos_size_t scm_size, daos_size_t nvme_size, daos_prop_t* prop, d_rank_list_t* svc, uuid_t uuid) {

    std::string pool_name;
    eckit::PathName label_symlink_path;

    if (prop != NULL) {

        if (prop->dpp_nr != 1)
            NOTIMP;
        if (prop->dpp_entries[0].dpe_type != DAOS_PROP_PO_LABEL)
            NOTIMP;

        struct daos_prop_entry* entry = &prop->dpp_entries[0];

        if (entry == NULL)
            NOTIMP;

        pool_name = std::string(entry->dpe_str);

        label_symlink_path = dummy_daos_root() / pool_name;

        if (label_symlink_path.exists())
            return -1;
    }

    /// @note: copied from LocalPathName::unique. Ditched StaticMutex as dummy DAOS is not thread safe

    std::string hostname = eckit::Main::hostname();

    static unsigned long long n = (((unsigned long long)::getpid()) << 32);

    static std::string format = "%Y%m%d.%H%M%S";
    std::ostringstream os;
    os << eckit::TimeStamp(format) << '.' << hostname << '.' << n++;

    std::string name = os.str();

    while (::access(name.c_str(), F_OK) == 0) {
        std::ostringstream os;
        os << eckit::TimeStamp(format) << '.' << hostname << '.' << n++;
        name = os.str();
    }

    eckit::MD5 md5(name);
    uint64_t hi = std::stoull(md5.digest().substr(0, 8), nullptr, 16);
    uint64_t lo = std::stoull(md5.digest().substr(8, 16), nullptr, 16);
    ::memcpy(&uuid[0], &hi, sizeof(hi));
    ::memcpy(&uuid[8], &lo, sizeof(lo));

    char pool_uuid_cstr[37] = "";
    uuid_unparse(uuid, pool_uuid_cstr);

    eckit::PathName pool_path = dummy_daos_root() / pool_uuid_cstr;

    if (pool_path.exists())
        throw eckit::SeriousBug("UUID clash in pool create");

    pool_path.mkdir();

    if (prop == NULL)
        return 0;

    if (::symlink(pool_path.path().c_str(), label_symlink_path.path().c_str()) < 0) {

        if (errno == EEXIST) {  // link path already exists due to race condition

            eckit::testing::deldir(pool_path);
            return -1;
        }
        else {  // symlink fails for unknown reason

            throw eckit::FailedSystemCall(std::string("symlink ") + pool_path.path() + " " + label_symlink_path.path());
        }
    }

    return 0;
}

int dmg_pool_destroy(const char* dmg_config_file, const uuid_t uuid, const char* grp, int force) {

    char uuid_str[37] = "";

    uuid_unparse(uuid, uuid_str);

    eckit::PathName pool_path = dummy_daos_root() / uuid_str;

    if (!pool_path.exists())
        return -1;

    std::vector<eckit::PathName> files;
    std::vector<eckit::PathName> dirs;
    dummy_daos_root().children(files, dirs);

    /// @note: all pool labels (symlinks) are visited and deleted if they point to the
    ///        specified pool UUID. None will match if the pool was not labelled.

    for (auto& f : files) {
        try {

            if (f.isLink() && f.realName().baseName() == pool_path.baseName())
                f.unlink();
        }
        catch (eckit::FailedSystemCall& e) {

            if (f.exists())
                throw;
        }
    }

    eckit::testing::deldir(pool_path);

    return 0;
}

//----------------------------------------------------------------------------------------------------------------------

}  // extern "C"

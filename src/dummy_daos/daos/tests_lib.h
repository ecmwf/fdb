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
 * @file   tests_lib.h
 * @author Nicolau Manubens
 * @date   Jun 2022
 */

#ifndef fdb5_dummy_daos_daos_tests_lib_H
#define fdb5_dummy_daos_daos_tests_lib_H

#include <uuid.h>

#include "../daos.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32_t d_rank_t;

typedef struct {
    d_rank_t *rl_ranks;
    uint32_t rl_nr;
} d_rank_list_t;

int dmg_pool_create(const char *dmg_config_file,
                    uid_t uid, gid_t gid, const char *grp,
                    const d_rank_list_t *tgts,
                    daos_size_t scm_size, daos_size_t nvme_size,
                    daos_prop_t *prop,
                    d_rank_list_t *svc, uuid_t uuid);

int dmg_pool_destroy(const char *dmg_config_file,
                     const uuid_t uuid, const char *grp, int force);

#ifdef __cplusplus
}  // extern "C"
#endif /* end ifdef __cplusplus */

#endif /* fdb5_dummy_daos_daos_tests_lib_H */
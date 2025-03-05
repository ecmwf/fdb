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

#pragma once

#include <uuid/uuid.h>

#include "../daos.h"

//----------------------------------------------------------------------------------------------------------------------

#define D_ALLOC_ARRAY(ptr, count) (ptr) = (__typeof__(ptr))calloc((count), (sizeof(*ptr)));
#define D_ALLOC_PTR(ptr) D_ALLOC_ARRAY(ptr, 1)
#define D_FREE(ptr)   \
    ({                \
        free(ptr);    \
        (ptr) = NULL; \
    })
#define D_STRNDUP(ptr, s, n) (ptr) = strndup(s, n);

//----------------------------------------------------------------------------------------------------------------------

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32_t d_rank_t;

typedef struct {
    d_rank_t* rl_ranks;
    uint32_t rl_nr;
} d_rank_list_t;

//----------------------------------------------------------------------------------------------------------------------

daos_prop_t* daos_prop_alloc(uint32_t entries_nr);

void daos_prop_free(daos_prop_t* prop);

int dmg_pool_create(const char* dmg_config_file, uid_t uid, gid_t gid, const char* grp, const d_rank_list_t* tgts,
                    daos_size_t scm_size, daos_size_t nvme_size, daos_prop_t* prop, d_rank_list_t* svc, uuid_t uuid);

int dmg_pool_destroy(const char* dmg_config_file, const uuid_t uuid, const char* grp, int force);

//----------------------------------------------------------------------------------------------------------------------

#ifdef __cplusplus
}  // extern "C"
#endif /* end ifdef __cplusplus */

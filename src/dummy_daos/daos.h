/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   daos.h
/// @author Nicolau Manubens
/// @date   Jun 2022

#ifndef fdb5_dummy_daos_daos_H
#define fdb5_dummy_daos_daos_H

#include <stdlib.h>
#include <stdint.h>

#define DAOS_PC_RW 0
#define DAOS_COO_RW 0
#define DAOS_OO_RW 0
#define OC_S1 1ULL
#define DAOS_TX_NONE (daos_handle_t){NULL}

#ifdef __cplusplus
extern "C" {
#endif

struct daos_handle_internal_t;
typedef struct daos_handle_internal_t daos_handle_internal_t;

typedef struct {
    daos_handle_internal_t* impl;
} daos_handle_t;

// typedef struct daos_handle_internal_t * daos_handle_t;

enum daos_otype_t {
    DAOS_OT_KV_HASHED = 8
};

typedef uint64_t daos_size_t;

typedef struct {
    uint64_t hi;
    uint64_t lo;
} daos_obj_id_t;

typedef uint32_t daos_oclass_id_t;
typedef uint16_t daos_oclass_hints_t;

typedef void daos_event_t;
typedef void daos_pool_info_t;
typedef void daos_prop_t;
typedef void daos_cont_info_t;
//typedef void uuid_t;

// describe object-space target range
typedef uint64_t daos_off_t;
typedef struct {
    daos_off_t rg_idx;
    daos_size_t rg_len;
} daos_range_t;
typedef struct {
    daos_size_t arr_nr;
    daos_range_t *arr_rgs;
    daos_size_t arr_nr_short_read;
    daos_size_t arr_nr_read;
} daos_array_iod_t;

// describe memory-space target region
typedef struct {
    void *iov_buf;
    size_t iov_buf_len;
    size_t iov_len;
} d_iov_t;
typedef struct {
    uint32_t sg_nr;
    uint32_t sg_nr_out;
    d_iov_t *sg_iovs;
} d_sg_list_t;

int
daos_init(void);
//daos_init()

int
daos_fini(void);
//daos_fini()

int
daos_pool_connect(const char *pool, const char *sys, unsigned int flags,
                  daos_handle_t *poh, daos_pool_info_t *info, daos_event_t *ev);

int
daos_pool_disconnect(daos_handle_t poh, daos_event_t *ev);

int
daos_cont_create_with_label(daos_handle_t poh, const char *label,
                            daos_prop_t *cont_prop, void *uuid,
                            daos_event_t *ev);

int
daos_cont_open(daos_handle_t poh, const char *cont, unsigned int flags, daos_handle_t *coh,
               daos_cont_info_t *info, daos_event_t *ev);

int
daos_cont_close(daos_handle_t coh, daos_event_t *ev);

int
daos_cont_alloc_oids(daos_handle_t coh, daos_size_t num_oids, uint64_t *oid,
                     daos_event_t *ev);

int
daos_obj_generate_oid(daos_handle_t coh, daos_obj_id_t *oid,
                      enum daos_otype_t type, daos_oclass_id_t cid,
                      daos_oclass_hints_t hints, uint32_t args);

int
daos_kv_open(daos_handle_t coh, daos_obj_id_t oid, unsigned int mode,
             daos_handle_t *oh, daos_event_t *ev);

int
daos_obj_close(daos_handle_t oh, daos_event_t *ev);

int
daos_kv_put(daos_handle_t oh, daos_handle_t th, uint64_t flags, const char *key,
            daos_size_t size, const void *buf, daos_event_t *ev);

int
daos_kv_get(daos_handle_t oh, daos_handle_t th, uint64_t flags, const char *key,
            daos_size_t *size, void *buf, daos_event_t *ev);

int
daos_array_generate_oid(daos_handle_t coh, daos_obj_id_t *oid, bool add_attr, daos_oclass_id_t cid,
                        daos_oclass_hints_t hints, uint32_t args);

int
daos_array_create(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th,
                  daos_size_t cell_size, daos_size_t chunk_size,
                  daos_handle_t *oh, daos_event_t *ev);

int
daos_array_open(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th,
                unsigned int mode, daos_size_t *cell_size,
                daos_size_t *chunk_size, daos_handle_t *oh, daos_event_t *ev);

int
daos_array_close(daos_handle_t oh, daos_event_t *ev);

static inline void
d_iov_set(d_iov_t *iov, void *buf, size_t size) {
    iov->iov_buf = buf;
    iov->iov_len = iov->iov_buf_len = size;
}

int
daos_array_write(daos_handle_t oh, daos_handle_t th, daos_array_iod_t *iod,
                 d_sg_list_t *sgl, daos_event_t *ev);

int
daos_array_get_size(daos_handle_t oh, daos_handle_t th, daos_size_t *size,
                    daos_event_t *ev);

int
daos_array_read(daos_handle_t oh, daos_handle_t th, daos_array_iod_t *iod,
                d_sg_list_t *sgl, daos_event_t *ev);

}  // extern "C"

#endif //fdb5_dummy_daos_daos_H
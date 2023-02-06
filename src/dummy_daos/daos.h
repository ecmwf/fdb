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
 * @file   daos.h
 * @author Nicolau Manubens
 * @date   Jun 2022
 */

#ifndef fdb5_dummy_daos_daos_H
#define fdb5_dummy_daos_daos_H

#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <uuid/uuid.h>
#include <stdbool.h>

#define DAOS_PC_RW 0
#define DAOS_COO_RW 0
#define DAOS_OO_RW 0
#define OC_S1 1ULL
#define OC_S2 2ULL
#define OC_SX ((1 << 16UL) - 1)
#define OC_RESERVED 1 << 30
#define DAOS_TX_NONE (daos_handle_t){NULL}
#define DAOS_PROP_LABEL_MAX_LEN (127)
#define DAOS_PROP_ENTRIES_MAX_NR (128)

#define D_ALLOC_ARRAY(ptr, count) (ptr) = (__typeof__(ptr))calloc((count), (sizeof(*ptr)));
#define D_ALLOC_PTR(ptr) D_ALLOC_ARRAY(ptr, 1)
#define D_FREE(ptr) ({ free(ptr); (ptr) = NULL; })
#define D_STRNDUP(ptr, s, n) (ptr) = strndup(s, n);

#ifdef __cplusplus
extern "C" {
#endif

struct daos_handle_internal_t;
typedef struct daos_handle_internal_t daos_handle_internal_t;

typedef struct {
    daos_handle_internal_t* impl;
} daos_handle_t;

/* typedef struct daos_handle_internal_t * daos_handle_t; */

enum daos_otype_t {
    DAOS_OT_KV_HASHED = 8,
    DAOS_OT_ARRAY = 11,
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
typedef void daos_cont_info_t;

/* describe object-space target range */
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

/* describe memory-space target region */
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

/* pool properties */

typedef char* d_string_t;

struct daos_prop_entry {
    uint32_t dpe_type;
    uint32_t dpe_reserv;
    union {
        uint64_t dpe_val;
        d_string_t dpe_str;
        void *dpe_val_ptr;
    };
};

typedef struct {
    uint32_t dpp_nr;
    uint32_t dpp_reserv;
    struct daos_prop_entry *dpp_entries;
} daos_prop_t;

enum daos_pool_props {
    DAOS_PROP_PO_LABEL
};

/* functions */

int daos_init(void);

int daos_fini(void);

int daos_pool_connect2(const char *pool, const char *sys, unsigned int flags,
                       daos_handle_t *poh, daos_pool_info_t *info, daos_event_t *ev);

int daos_pool_disconnect(daos_handle_t poh, daos_event_t *ev);

int daos_cont_create(daos_handle_t poh, const uuid_t uuid, daos_prop_t *cont_prop, daos_event_t *ev);

int daos_cont_create_with_label(daos_handle_t poh, const char *label,
                                daos_prop_t *cont_prop, uuid_t *uuid,
                                daos_event_t *ev);

int daos_cont_open2(daos_handle_t poh, const char *cont, unsigned int flags, daos_handle_t *coh,
                    daos_cont_info_t *info, daos_event_t *ev);

int daos_cont_close(daos_handle_t coh, daos_event_t *ev);

int daos_cont_alloc_oids(daos_handle_t coh, daos_size_t num_oids, uint64_t *oid,
                         daos_event_t *ev);

int daos_obj_generate_oid(daos_handle_t coh, daos_obj_id_t *oid,
                          enum daos_otype_t type, daos_oclass_id_t cid,
                          daos_oclass_hints_t hints, uint32_t args);

int daos_kv_open(daos_handle_t coh, daos_obj_id_t oid, unsigned int mode,
                 daos_handle_t *oh, daos_event_t *ev);

int daos_obj_close(daos_handle_t oh, daos_event_t *ev);

int daos_kv_put(daos_handle_t oh, daos_handle_t th, uint64_t flags, const char *key,
                daos_size_t size, const void *buf, daos_event_t *ev);

int daos_kv_get(daos_handle_t oh, daos_handle_t th, uint64_t flags, const char *key,
                daos_size_t *size, void *buf, daos_event_t *ev);

int daos_array_generate_oid(daos_handle_t coh, daos_obj_id_t *oid, bool add_attr, daos_oclass_id_t cid,
                            daos_oclass_hints_t hints, uint32_t args);

int daos_array_create(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th,
                      daos_size_t cell_size, daos_size_t chunk_size,
                      daos_handle_t *oh, daos_event_t *ev);

int daos_array_open(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th,
                    unsigned int mode, daos_size_t *cell_size,
                    daos_size_t *chunk_size, daos_handle_t *oh, daos_event_t *ev);

int daos_array_close(daos_handle_t oh, daos_event_t *ev);

static inline void d_iov_set(d_iov_t *iov, void *buf, size_t size) {
    iov->iov_buf = buf;
    iov->iov_len = iov->iov_buf_len = size;
}

int daos_array_write(daos_handle_t oh, daos_handle_t th, daos_array_iod_t *iod,
                     d_sg_list_t *sgl, daos_event_t *ev);

int daos_array_get_size(daos_handle_t oh, daos_handle_t th, daos_size_t *size,
                        daos_event_t *ev);

int daos_array_read(daos_handle_t oh, daos_handle_t th, daos_array_iod_t *iod,
                    d_sg_list_t *sgl, daos_event_t *ev);

daos_prop_t* daos_prop_alloc(uint32_t entries_nr);

void daos_prop_free(daos_prop_t *prop);

/*
 * The following is code for backwards-compatibility with older DAOS API versions where
 * some API calls accepted slightly different types for some of the arguments.
 * It has been mostly copied from daos.h in DAOS v2.0.3.
 */

#ifdef __cplusplus
}  // extern "C"

#define daos_pool_connect daos_pool_connect_cpp
static inline int daos_pool_connect_cpp(const char *pool, const char *sys, unsigned int flags, daos_handle_t *poh,
                                        daos_pool_info_t *info, daos_event_t *ev)
{
        return daos_pool_connect2(pool, sys, flags, poh, info, ev);
}

static inline int daos_pool_connect_cpp(const uuid_t pool, const char *sys, unsigned int flags, daos_handle_t *poh,
                                        daos_pool_info_t *info, daos_event_t *ev)
{
        char str[37];

        uuid_unparse(pool, str);
        return daos_pool_connect2(str, sys, flags, poh, info, ev);
}

#define daos_cont_open daos_cont_open_cpp
static inline int daos_cont_open_cpp(daos_handle_t poh, const char *cont, unsigned int flags, daos_handle_t *coh,
                                     daos_cont_info_t *info, daos_event_t *ev) {

    return daos_cont_open2(poh, cont, flags, coh, info, ev);

}

static inline int daos_cont_open_cpp(daos_handle_t poh, const uuid_t cont, unsigned int flags, daos_handle_t *coh,
                                     daos_cont_info_t *info, daos_event_t *ev) {

    char str[37];

    uuid_unparse(cont, str);
    return daos_cont_open2(poh, str, flags, coh, info, ev);

}

#else /* not __cplusplus */

#define d_is_uuid(var)                                                          \
    (__builtin_types_compatible_p(__typeof__(var), uuid_t) ||                   \
     __builtin_types_compatible_p(__typeof__(var), unsigned char *) ||          \
     __builtin_types_compatible_p(__typeof__(var), const unsigned char *) ||    \
     __builtin_types_compatible_p(__typeof__(var), const uuid_t))

#define d_is_string(var)                                                        \
    (__builtin_types_compatible_p(__typeof__(var), char *) ||                   \
     __builtin_types_compatible_p(__typeof__(var), const char *) ||             \
     __builtin_types_compatible_p(__typeof__(var), const char []) ||            \
     __builtin_types_compatible_p(__typeof__(var), char []))

#define daos_pool_connect(po, ...)                                      \
    ({                                                              \
            int _ret;                                               \
            char _str[37];                                          \
            const char *__str = NULL;                               \
            if (d_is_string(po)) {                                  \
                    __str = (const char *)(po);                     \
            } else if (d_is_uuid(po)) {                             \
                    uuid_unparse((unsigned char *)(po), _str);      \
                    __str = _str;                                   \
            }                                                       \
            _ret = daos_pool_connect2(__str, __VA_ARGS__);          \
            _ret;                                                   \
    })

#define daos_cont_open(poh, co, ...)                        \
    ({                                                      \
        int _ret;                                           \
        char _str[37];                                      \
        const char *__str = NULL;                           \
        if (d_is_string(co)) {                              \
            __str = (const char *)(co);                     \
        } else if (d_is_uuid(co)) {                         \
            uuid_unparse((unsigned char *)(co), _str);      \
            __str = _str;                                   \
        }                                                   \
        _ret = daos_cont_open2((poh), __str, __VA_ARGS__);  \
        _ret;                                               \
    })

#endif /* end not __cplusplus */

#endif /* fdb5_dummy_daos_daos_H */


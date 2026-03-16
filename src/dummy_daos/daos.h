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

#pragma once

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <uuid/uuid.h>

//----------------------------------------------------------------------------------------------------------------------

#define DAOS_API_VERSION_MAJOR 2
#define DAOS_API_VERSION_MINOR 0
#define DAOS_API_VERSION_FIX 3

#define DAOS_PC_RW 0
#define DAOS_COO_RW 0
#define DAOS_OO_RW 0

/** 32 bits for DAOS internal use */
#define OID_FMT_INTR_BITS 32
#define OID_FMT_TYPE_BITS 8
#define OID_FMT_CLASS_BITS 8
#define OID_FMT_META_BITS 16

#define OID_FMT_TYPE_SHIFT (64 - OID_FMT_TYPE_BITS)
#define OID_FMT_CLASS_SHIFT (OID_FMT_TYPE_SHIFT - OID_FMT_CLASS_BITS)
#define OID_FMT_META_SHIFT (OID_FMT_CLASS_SHIFT - OID_FMT_META_BITS)

#define OID_FMT_TYPE_MAX ((1ULL << OID_FMT_TYPE_BITS) - 1)
#define OID_FMT_CLASS_MAX ((1ULL << OID_FMT_CLASS_BITS) - 1)
#define OID_FMT_META_MAX ((1ULL << OID_FMT_META_BITS) - 1)

#define OID_FMT_TYPE_MASK (OID_FMT_TYPE_MAX << OID_FMT_TYPE_SHIFT)
#define OID_FMT_CLASS_MASK (OID_FMT_CLASS_MAX << OID_FMT_CLASS_SHIFT)
#define OID_FMT_META_MASK (OID_FMT_META_MAX << OID_FMT_META_SHIFT)

#define OC_RESERVED 1 << 30
#define OC_REDUN_SHIFT 24
#define OBJ_CLASS_DEF(redun, grp_nr) ((redun << OC_REDUN_SHIFT) | grp_nr)
#define MAX_NUM_GROUPS ((1 << 16UL) - 1)

#define DAOS_TX_NONE  \
    (daos_handle_t) { \
        NULL          \
    }
#define DAOS_PROP_LABEL_MAX_LEN (127)
#define DAOS_PROP_ENTRIES_MAX_NR (128)

#define DAOS_ANCHOR_BUF_MAX 104
#define DAOS_ANCHOR_INIT \
    {.da_type = DAOS_ANCHOR_TYPE_ZERO, .da_shard = 0, .da_flags = 0, .da_sub_anchors = 0, .da_buf = {0}}

#define DER_EXIST 1004
#define DER_NONEXIST 1005

//----------------------------------------------------------------------------------------------------------------------

enum daos_otype_t {
    DAOS_OT_KV_HASHED = 8,
    DAOS_OT_ARRAY = 11,
    DAOS_OT_ARRAY_BYTE = 13,
};

enum daos_pool_props {
    DAOS_PROP_PO_LABEL,
    DAOS_PROP_CO_LABEL
};

enum daos_snapshot_opts {
    DAOS_SNAP_OPT_CR = (1 << 0),
    DAOS_SNAP_OPT_OIT = (1 << 1)
};

enum daos_obj_redun {
    OR_RP_1 = 1
};

enum {
    OC_S1 = OBJ_CLASS_DEF(OR_RP_1, 1ULL),
    OC_S2 = OBJ_CLASS_DEF(OR_RP_1, 2ULL),
    OC_SX = OBJ_CLASS_DEF(OR_RP_1, MAX_NUM_GROUPS)
};

#ifdef __cplusplus
extern "C" {
#endif

struct daos_handle_internal_t;
typedef struct daos_handle_internal_t daos_handle_internal_t;

typedef struct {
    daos_handle_internal_t* impl;
} daos_handle_t;

/* typedef struct daos_handle_internal_t * daos_handle_t; */

typedef uint64_t daos_size_t;

typedef struct {
    uint64_t lo;
    uint64_t hi;
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
    daos_range_t* arr_rgs;
    daos_size_t arr_nr_short_read;
    daos_size_t arr_nr_read;
} daos_array_iod_t;

/* describe memory-space target region */
typedef struct {
    void* iov_buf;
    size_t iov_buf_len;
    size_t iov_len;
} d_iov_t;
typedef struct {
    uint32_t sg_nr;
    uint32_t sg_nr_out;
    d_iov_t* sg_iovs;
} d_sg_list_t;

/* pool properties */

typedef char* d_string_t;

struct daos_prop_entry {
    uint32_t dpe_type;
    uint32_t dpe_reserv;
    union {
        uint64_t dpe_val;
        d_string_t dpe_str;
        void* dpe_val_ptr;
    };
};

typedef struct {
    uint32_t dpp_nr;
    uint32_t dpp_reserv;
    struct daos_prop_entry* dpp_entries;
} daos_prop_t;

/* cont info */

struct daos_pool_cont_info {
    uuid_t pci_uuid;
    char pci_label[DAOS_PROP_LABEL_MAX_LEN + 2];
};

/* kv list */

typedef struct {
    uint16_t da_type;
    uint16_t da_shard;
    uint32_t da_flags;
    uint64_t da_sub_anchors;
    uint8_t da_buf[DAOS_ANCHOR_BUF_MAX];
} daos_anchor_t;

typedef struct {
    daos_size_t kd_key_len;
    uint32_t kd_val_type;
} daos_key_desc_t;

typedef enum {
    DAOS_ANCHOR_TYPE_ZERO = 0,
    DAOS_ANCHOR_TYPE_HKEY = 1,
    DAOS_ANCHOR_TYPE_KEY = 2,
    DAOS_ANCHOR_TYPE_EOF = 3,
} daos_anchor_type_t;

/* cont list oids */

typedef uint64_t daos_epoch_t;

typedef struct {
    daos_epoch_t epr_lo;
    daos_epoch_t epr_hi;
} daos_epoch_range_t;

//----------------------------------------------------------------------------------------------------------------------

/* functions */

int daos_init(void);

int daos_fini(void);

int daos_pool_connect(const char* pool, const char* sys, unsigned int flags, daos_handle_t* poh, daos_pool_info_t* info,
                      daos_event_t* ev);

int daos_pool_disconnect(daos_handle_t poh, daos_event_t* ev);

/*
 * warning: the daos_pool_list_cont API call is not fully implemented. Only the pci_label
 * member of the info structs is populated in all cases. The pci_uuid member is only
 * populated for existing containers which were created without a label.
 */
int daos_pool_list_cont(daos_handle_t poh, daos_size_t* ncont, struct daos_pool_cont_info* cbuf, daos_event_t* ev);

int daos_cont_create(daos_handle_t poh, uuid_t* uuid, daos_prop_t* cont_prop, daos_event_t* ev);

int daos_cont_create_with_label(daos_handle_t poh, const char* label, daos_prop_t* cont_prop, uuid_t* uuid,
                                daos_event_t* ev);

int daos_cont_destroy(daos_handle_t poh, const char* cont, int force, daos_event_t* ev);

int daos_cont_open(daos_handle_t poh, const char* cont, unsigned int flags, daos_handle_t* coh, daos_cont_info_t* info,
                   daos_event_t* ev);

int daos_cont_close(daos_handle_t coh, daos_event_t* ev);

int daos_cont_alloc_oids(daos_handle_t coh, daos_size_t num_oids, uint64_t* oid, daos_event_t* ev);

int daos_obj_generate_oid(daos_handle_t coh, daos_obj_id_t* oid, enum daos_otype_t type, daos_oclass_id_t cid,
                          daos_oclass_hints_t hints, uint32_t args);

int daos_kv_open(daos_handle_t coh, daos_obj_id_t oid, unsigned int mode, daos_handle_t* oh, daos_event_t* ev);

int daos_kv_destroy(daos_handle_t oh, daos_handle_t th, daos_event_t* ev);

int daos_obj_close(daos_handle_t oh, daos_event_t* ev);

int daos_kv_put(daos_handle_t oh, daos_handle_t th, uint64_t flags, const char* key, daos_size_t size, const void* buf,
                daos_event_t* ev);

int daos_kv_get(daos_handle_t oh, daos_handle_t th, uint64_t flags, const char* key, daos_size_t* size, void* buf,
                daos_event_t* ev);

int daos_kv_remove(daos_handle_t oh, daos_handle_t th, uint64_t flags, const char* key, daos_event_t* ev);

int daos_kv_list(daos_handle_t oh, daos_handle_t th, uint32_t* nr, daos_key_desc_t* kds, d_sg_list_t* sgl,
                 daos_anchor_t* anchor, daos_event_t* ev);

static inline bool daos_anchor_is_eof(daos_anchor_t* anchor) {
    return anchor->da_type == DAOS_ANCHOR_TYPE_EOF;
}

int daos_array_generate_oid(daos_handle_t coh, daos_obj_id_t* oid, bool add_attr, daos_oclass_id_t cid,
                            daos_oclass_hints_t hints, uint32_t args);

int daos_array_create(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th, daos_size_t cell_size,
                      daos_size_t chunk_size, daos_handle_t* oh, daos_event_t* ev);

int daos_array_destroy(daos_handle_t oh, daos_handle_t th, daos_event_t* ev);

int daos_array_open(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th, unsigned int mode, daos_size_t* cell_size,
                    daos_size_t* chunk_size, daos_handle_t* oh, daos_event_t* ev);

int daos_array_open_with_attr(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th, unsigned int mode,
                              daos_size_t cell_size, daos_size_t chunk_size, daos_handle_t* oh, daos_event_t* ev);

int daos_array_close(daos_handle_t oh, daos_event_t* ev);

static inline void d_iov_set(d_iov_t* iov, void* buf, size_t size) {
    iov->iov_buf = buf;
    iov->iov_len = iov->iov_buf_len = size;
}

int daos_array_write(daos_handle_t oh, daos_handle_t th, daos_array_iod_t* iod, d_sg_list_t* sgl, daos_event_t* ev);

int daos_array_get_size(daos_handle_t oh, daos_handle_t th, daos_size_t* size, daos_event_t* ev);

int daos_array_read(daos_handle_t oh, daos_handle_t th, daos_array_iod_t* iod, d_sg_list_t* sgl, daos_event_t* ev);

int daos_cont_create_snap_opt(daos_handle_t coh, daos_epoch_t* epoch, char* name, enum daos_snapshot_opts opts,
                              daos_event_t* ev);

int daos_cont_destroy_snap(daos_handle_t coh, daos_epoch_range_t epr, daos_event_t* ev);

int daos_oit_open(daos_handle_t coh, daos_epoch_t epoch, daos_handle_t* oh, daos_event_t* ev);

int daos_oit_close(daos_handle_t oh, daos_event_t* ev);

int daos_oit_list(daos_handle_t oh, daos_obj_id_t* oids, uint32_t* oids_nr, daos_anchor_t* anchor, daos_event_t* ev);

//----------------------------------------------------------------------------------------------------------------------

#ifdef __cplusplus
}  // extern "C"
#endif

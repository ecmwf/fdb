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
 * @file   daos.cc
 * @author Nicolau Manubens
 * @date   Jun 2022
 */

#include <cstring>
#include <string>
#include <iomanip>
#include <unistd.h>
#include <limits.h>

#include "eckit/runtime/Main.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/io/Length.h"
#include "eckit/io/FileHandle.h"
#include "eckit/types/UUID.h"

#include "daos.h"
#include "dummy_daos.h"

using eckit::PathName;

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

typedef struct daos_handle_internal_t {
    PathName path;
} daos_handle_internal_t;

int daos_init() {
    const char* argv[2] = {"dummy-daos-api", 0};
    eckit::Main::initialise(1, const_cast<char**>(argv));
    PathName root = dummy_daos_root();
    root.mkdir();
    return 0;
}

int daos_fini() {
    return 0;
}

int daos_pool_connect2(const char *pool, const char *sys, unsigned int flags,
                       daos_handle_t *poh, daos_pool_info_t *info, daos_event_t *ev) {

    poh->impl = nullptr;

    if (sys != NULL) NOTIMP;
    if (flags != DAOS_PC_RW) NOTIMP;
    if (info != NULL) NOTIMP;
    if (ev != NULL) NOTIMP;

    std::unique_ptr<daos_handle_internal_t> impl(new daos_handle_internal_t);
    impl->path = dummy_daos_root() / pool;

    if (!(impl->path).exists()) {
        return -1;
    }

    poh->impl = impl.release();
    return 0;

}

int daos_pool_disconnect(daos_handle_t poh, daos_event_t *ev) {

    delete poh.impl;

    if (ev != NULL) NOTIMP;

    return 0;

}

int daos_pool_list_cont(daos_handle_t poh, daos_size_t *ncont,
                        struct daos_pool_cont_info *cbuf, daos_event_t *ev) {

    if (ev != NULL) NOTIMP;

    daos_size_t n(*ncont);

    std::vector<eckit::PathName> files;
    std::vector<eckit::PathName> dirs;

    poh.impl->path.children(files, dirs);

    *ncont = dirs.size();

    if (cbuf == NULL) return 0;

    daos_size_t nfound = 0;

    for (const auto& dir : dirs) {

        ++nfound;

        if (nfound > n) return -1;

        std::string contname = dir.baseName().asString();
        const char* contname_cstr = contname.c_str();
        ASSERT(strlen(contname_cstr) <= DAOS_PROP_LABEL_MAX_LEN);

        strncpy(cbuf[nfound - 1].pci_label, contname_cstr, DAOS_PROP_LABEL_MAX_LEN + 1);

        uuid_t uuid = {0};
        if (uuid_parse(contname_cstr, uuid) == 0)
            uuid_copy(cbuf[nfound - 1].pci_uuid, uuid);

    }

    return 0;

}

int daos_cont_create(daos_handle_t poh, const uuid_t uuid, daos_prop_t *cont_prop, daos_event_t *ev) {

    char label[37];

    uuid_unparse(uuid, label);

    return daos_cont_create_with_label(poh, label, cont_prop, NULL, ev);

}

int daos_cont_create_with_label(daos_handle_t poh, const char *label,
                                daos_prop_t *cont_prop, uuid_t *uuid,
                                daos_event_t *ev) {

    if (cont_prop != NULL) NOTIMP;
    if (uuid != NULL) NOTIMP;
    if (ev != NULL) NOTIMP;

    //TODO: make a random directory as in pool create, and create symlink.
    //      This would allow to implement the uuid parameter of this function,
    //      thus having containers with both a uuid and a label. Also would allow
    //      fully implementing daos_pool_list_cont.
    //      This would not be straightforward as that method requires generating
    //      a random uuid and then attempting creating a directory with that
    //      uuid as name, and it would imply losing atomicity and leaving room
    //      for race conditions in parallel container creation.
    //      The symlink creation would also leave room for race conditions in
    //      concurrent container creation plus destruction.

    ASSERT(strlen(label) <= DAOS_PROP_LABEL_MAX_LEN);

    (poh.impl->path / label).mkdir();

    return 0;

}

// in DAOS, an attempt to destroy a container with open handles results in error by default. This
// behavior is not implemented in dummy DAOS.
// in DAOS, an attempt to destroy a container with open handles with the force flag enabled closes
// open handles, and therefore ongoing/future operations on these handles fail. The contained objects 
// and the container are immediately destroyed. In dummy DAOS the open handles are implemented with 
// file descriptors, and these are left open. A remove operation is called on the corresponding file 
// names but the descriptors remain open. Therefore, in contrast to DAOS, ongoing/future operations on 
// these handles succeed, and the files are destroyed after the descriptors are closed. The folder 
// implementing the container, however, is immediately removed.

int daos_cont_destroy(daos_handle_t poh, const char *cont, int force, daos_event_t *ev) {

    if (force != 1) NOTIMP;
    if (ev != NULL) NOTIMP;

    eckit::PathName path{poh.impl->path / cont};
    eckit::PathName tmp_path{poh.impl->path / ("destroy_" + std::string(cont))};
    eckit::PathName::rename(path, tmp_path);

    deldir(tmp_path);

    return 0;

}

int daos_cont_open2(daos_handle_t poh, const char *cont, unsigned int flags, daos_handle_t *coh,
                    daos_cont_info_t *info, daos_event_t *ev) {

    if (flags != DAOS_COO_RW) NOTIMP;
    if (info != NULL) NOTIMP;
    if (ev != NULL) NOTIMP;

    std::unique_ptr<daos_handle_internal_t> impl(new daos_handle_internal_t);
    impl->path = poh.impl->path / cont;

    if (!(impl->path).exists()) {
        return -1;
    }

    coh->impl = impl.release();
    return 0;

}

int daos_cont_close(daos_handle_t coh, daos_event_t *ev) {

    delete coh.impl;

    if (ev != NULL) NOTIMP;

    return 0;

}

int daos_cont_alloc_oids(daos_handle_t coh, daos_size_t num_oids, uint64_t *oid,
                         daos_event_t *ev) {

    static uint64_t next_oid = 0;

    if (ev != NULL) NOTIMP;
    ASSERT(num_oids > (uint64_t) 0);

    // support for multi-node clients running dummy DAOS backed by a 
    // distributed file system
    std::string host = eckit::Main::instance().hostname();

    const char *host_cstr = host.c_str();

    uuid_t seed = {0};
    uuid_t uuid;

    uuid_generate_md5(uuid, seed, host_cstr, strlen(host_cstr));

    char uuid_cstr[37] = "";
    uuid_unparse(uuid, uuid_cstr);

    pid_t pid = getpid();

    // only 20 out of the 32 bits in pid_t are used to identify the calling process.
    // because of this, there could be oid clashes
    uint64_t pid_mask = 0x00000000000FFFFF;

    uint64_t oid_mask = 0x000000000FFFFFFF;
    ASSERT((next_oid + num_oids) <= oid_mask);

    *oid = next_oid;
    *oid |= (((uint64_t) pid) & pid_mask) << 28;
    *oid |= (((uint64_t) *(((unsigned char *) uuid) + 1)) << 48);
    *oid |= (((uint64_t) *((unsigned char *) uuid)) << 56);

    next_oid += num_oids;

    return 0;

}

int daos_obj_generate_oid(daos_handle_t coh, daos_obj_id_t *oid,
                          enum daos_otype_t type, daos_oclass_id_t cid,
                          daos_oclass_hints_t hints, uint32_t args) {

    if (type != DAOS_OT_KV_HASHED) NOTIMP;
    if (cid != OC_S1) NOTIMP;
    if (hints != 0) NOTIMP;
    if (args != 0) NOTIMP;

    oid->hi &= (uint64_t) 0x00000000FFFFFFFF;

    return 0;

}

int daos_kv_open(daos_handle_t coh, daos_obj_id_t oid, unsigned int mode,
                 daos_handle_t *oh, daos_event_t *ev) {

    if (mode != DAOS_OO_RW) NOTIMP;
    if (ev != NULL) NOTIMP;

    std::stringstream os;
    os << std::setw(16) << std::setfill('0') << std::hex << oid.hi;
    os << ".";
    os << std::setw(16) << std::setfill('0') << std::hex << oid.lo;

    std::unique_ptr<daos_handle_internal_t> impl(new daos_handle_internal_t);
    impl->path = coh.impl->path / os.str();

    impl->path.mkdir();

    oh->impl = impl.release();
    return 0;

}

int daos_obj_close(daos_handle_t oh, daos_event_t *ev) {

    delete oh.impl;

    if (ev != NULL) NOTIMP;

    return 0;

}

// daos_kv_put and get are only guaranteed to work if values of a same fixed size are put/get.
//   e.g. two racing processes could both openForWrite as part of daos_kv_put (and wipe file 
//   content), then one writes (puts) content of length 2*x, the other writes content of 
//   length x, but old content remains at the end from the first kv_put.
//   e.g. a process could openForRead as part of daos_kv_get (which retrieves content length),  
//   then another raching process could daos_kv_put of some content with different length, 
//   and then the first process would resume retrieval and obtain content of unexpected length.
// if so, daos_kv_put and get are transactional

int daos_kv_put(daos_handle_t oh, daos_handle_t th, uint64_t flags, const char *key,
                daos_size_t size, const void *buf, daos_event_t *ev) {

    if (th.impl != DAOS_TX_NONE.impl) NOTIMP;
    if (flags != 0) NOTIMP;
    if (ev != NULL) NOTIMP;

    eckit::FileHandle fh(oh.impl->path / key, true);
    fh.openForWrite(eckit::Length(size));
    eckit::AutoClose closer(fh);
    fh.write(buf, (long) size);

    return 0;

}

int daos_kv_get(daos_handle_t oh, daos_handle_t th, uint64_t flags, const char *key,
                daos_size_t *size, void *buf, daos_event_t *ev) {

    if (th.impl != DAOS_TX_NONE.impl) NOTIMP;
    if (flags != 0) NOTIMP;
    if (ev != NULL) NOTIMP;

    bool exists = (oh.impl->path / key).exists();

    if (!exists && buf != NULL) return -1;

    *size = 0;
    if (!exists) return 0;

    eckit::FileHandle fh(oh.impl->path / key);
    eckit::Length len = fh.size();
    *size = len;

    if (buf == NULL) return 0;

    fh.openForRead();
    eckit::AutoClose closer(fh);
    long res = fh.read(buf, len);
    ASSERT(eckit::Length(res) == len);

    return 0;

}

int daos_array_generate_oid(daos_handle_t coh, daos_obj_id_t *oid, bool add_attr, daos_oclass_id_t cid,
                            daos_oclass_hints_t hints, uint32_t args) {

    if (add_attr != true) NOTIMP;
    if (cid != OC_S1) NOTIMP;
    if (hints != 0) NOTIMP;
    if (args != 0) NOTIMP;

    oid->hi &= (uint64_t) 0x00000000FFFFFFFF;

    return 0;

}

int daos_array_create(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th,
                      daos_size_t cell_size, daos_size_t chunk_size,
                      daos_handle_t *oh, daos_event_t *ev) {

    if (th.impl != DAOS_TX_NONE.impl) NOTIMP;
    if (ev != NULL) NOTIMP;

    std::stringstream os;
    os << std::setw(16) << std::setfill('0') << std::hex << oid.hi;
    os << ".";
    os << std::setw(16) << std::setfill('0') << std::hex << oid.lo;

    std::unique_ptr<daos_handle_internal_t> impl(new daos_handle_internal_t);
    impl->path = coh.impl->path / os.str();

    impl->path.touch();

    oh->impl = impl.release();
    return 0;

}

int daos_array_open(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th,
                    unsigned int mode, daos_size_t *cell_size,
                    daos_size_t *chunk_size, daos_handle_t *oh, daos_event_t *ev) {

    if (th.impl != DAOS_TX_NONE.impl) NOTIMP;
    if (mode != DAOS_OO_RW) NOTIMP;
    if (ev != NULL) NOTIMP;

    *cell_size = 1;
    *chunk_size = (uint64_t) 1048576;

    std::stringstream os;
    os << std::setw(16) << std::setfill('0') << std::hex << oid.hi;
    os << ".";
    os << std::setw(16) << std::setfill('0') << std::hex << oid.lo;

    std::unique_ptr<daos_handle_internal_t> impl(new daos_handle_internal_t);
    impl->path = coh.impl->path / os.str();

    if (!impl->path.exists()) {
        return -1;
    }

    oh->impl = impl.release();
    return 0;

}

int daos_array_close(daos_handle_t oh, daos_event_t *ev) {

    delete oh.impl;

    if (ev != NULL) NOTIMP;

    return 0;

}

// daos_array_write and read have the same limitations and transactional behavior as 
// daos_kv_put and get

int daos_array_write(daos_handle_t oh, daos_handle_t th, daos_array_iod_t *iod,
                     d_sg_list_t *sgl, daos_event_t *ev) {

    if (th.impl != DAOS_TX_NONE.impl) NOTIMP;
    if (ev != NULL) NOTIMP;

    if (iod->arr_nr != 1) NOTIMP;

    if (sgl->sg_nr != 1) NOTIMP;
    // source memory len
    if (sgl->sg_iovs[0].iov_buf_len != sgl->sg_iovs[0].iov_len) NOTIMP;
    // source memory vs. target object len
    if (sgl->sg_iovs[0].iov_buf_len != iod->arr_rgs[0].rg_len) NOTIMP;

    //sgl->sg_iovs[0].iov_buf is a void * with the data to write
    //sgl->sg_iovs[0].iov_buf_len is a size_t with the source len
    //iod->arr_rgs[0].rg_idx is a uint64_t with the offset to write from

    eckit::FileHandle fh(oh.impl->path, true);

    //eckit::Length existing_len = fh.size();

    // if writing data to an already existing and populated file, if the data to write
    // is smaller than the file or the data has an offset, the holes will be left with
    // pre-existing data (openForAppend) rather than zero-d out (openForWrite)

    fh.openForAppend(eckit::Length(sgl->sg_iovs[0].iov_buf_len));
    eckit::AutoClose closer(fh);
    fh.seek(iod->arr_rgs[0].rg_idx);
    fh.write(sgl->sg_iovs[0].iov_buf, (long) sgl->sg_iovs[0].iov_buf_len);

    return 0;

}

int daos_array_get_size(daos_handle_t oh, daos_handle_t th, daos_size_t *size,
                        daos_event_t *ev) {

    if (th.impl != DAOS_TX_NONE.impl) NOTIMP;
    if (ev != NULL) NOTIMP;

    *size = (daos_size_t) oh.impl->path.size();

    return 0;
    
}

int daos_array_read(daos_handle_t oh, daos_handle_t th, daos_array_iod_t *iod,
                    d_sg_list_t *sgl, daos_event_t *ev) {

    if (th.impl != DAOS_TX_NONE.impl) NOTIMP;
    if (ev != NULL) NOTIMP;

    if (iod->arr_nr != 1) NOTIMP;

    if (sgl->sg_nr != 1) NOTIMP;
    // target memory len
    if (sgl->sg_iovs[0].iov_buf_len != sgl->sg_iovs[0].iov_len) NOTIMP;
    // target memory vs. source object len
    if (sgl->sg_iovs[0].iov_buf_len != iod->arr_rgs[0].rg_len) NOTIMP;

    //sgl->sg_iovs[0].iov_buf is a void * where to read the data into
    //iod->arr_rgs[0].rg_len is a size_t with the source size
    //iod->arr_rgs[0].rg_idx is a uint64_t with the offset to read from

    eckit::FileHandle fh(oh.impl->path);
    eckit::Length len = fh.size();
    fh.openForRead();
    eckit::AutoClose closer(fh);
    fh.seek(iod->arr_rgs[0].rg_idx);
    long res = fh.read(sgl->sg_iovs[0].iov_buf, iod->arr_rgs[0].rg_len);
    ASSERT(eckit::Length(res) == eckit::Length(iod->arr_rgs[0].rg_len));

    return 0;

}

daos_prop_t* daos_prop_alloc(uint32_t entries_nr) {

    daos_prop_t *prop;

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

void daos_prop_fini(daos_prop_t *prop) {
    int i;

    if (!prop->dpp_entries)
        goto out;

    for (i = 0; i < prop->dpp_nr; i++) {
        struct daos_prop_entry *entry;

        entry = &prop->dpp_entries[i];
        if (entry->dpe_type != DAOS_PROP_PO_LABEL) NOTIMP;
        D_FREE(entry->dpe_str);
    }

    D_FREE(prop->dpp_entries);
out:
    prop->dpp_nr = 0;
}

void daos_prop_free(daos_prop_t *prop) {
    if (prop == NULL)
        return;

    daos_prop_fini(prop);
    D_FREE(prop);
}

}  // extern "C"

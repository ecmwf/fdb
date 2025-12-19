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

#include <limits.h>
#include <unistd.h>
#include <cstring>
#include <iomanip>
#include <string>

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/FileHandle.h"
#include "eckit/io/Length.h"
#include "eckit/log/TimeStamp.h"
#include "eckit/runtime/Main.h"
#include "eckit/types/UUID.h"
#include "eckit/utils/MD5.h"

#include "daos.h"
#include "dummy_daos.h"

using eckit::PathName;

namespace {
void deldir(eckit::PathName& p) {
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
}  // namespace

extern "C" {

//----------------------------------------------------------------------------------------------------------------------

typedef struct daos_handle_internal_t {
    PathName path;
} daos_handle_internal_t;

//----------------------------------------------------------------------------------------------------------------------

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

int daos_pool_connect(const char* pool, const char* sys, unsigned int flags, daos_handle_t* poh, daos_pool_info_t* info,
                      daos_event_t* ev) {

    poh->impl = nullptr;

    if (sys != NULL) {
        NOTIMP;
    }
    if (flags != DAOS_PC_RW) {
        NOTIMP;
    }
    if (info != NULL) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    eckit::PathName path = dummy_daos_root() / pool;
    eckit::PathName realpath{dummy_daos_root()};

    uuid_t uuid = {0};
    if (uuid_parse(pool, uuid) == 0) {

        realpath /= pool;
    }
    else {

        try {

            ASSERT(path.isLink());
            realpath /= path.realName().baseName();
        }
        catch (eckit::FailedSystemCall& e) {

            if (path.exists()) {
                throw;
            }
            return -1;
        }
    }

    if (!realpath.exists()) {
        return -1;
    }

    auto impl = std::make_unique<daos_handle_internal_t>();
    impl->path = realpath;
    poh->impl = impl.release();

    return 0;
}

int daos_pool_disconnect(daos_handle_t poh, daos_event_t* ev) {

    ASSERT(poh.impl);
    delete poh.impl;

    if (ev != NULL) {
        NOTIMP;
    }

    return 0;
}

int daos_pool_list_cont(daos_handle_t poh, daos_size_t* ncont, struct daos_pool_cont_info* cbuf, daos_event_t* ev) {

    ASSERT(poh.impl);
    if (ev != NULL) {
        NOTIMP;
    }

    daos_size_t n(*ncont);

    std::vector<eckit::PathName> files;
    std::vector<eckit::PathName> dirs;

    poh.impl->path.children(files, dirs);

    *ncont = files.size();

    if (cbuf == NULL) {
        return 0;
    }

    if (files.size() > n) {
        return -1;
    }

    daos_size_t nfound = 0;

    for (auto& f : files) {

        if (f.exists()) {  /// @todo: is the check in this line really necessary, given the try-catch below?

            ++nfound;

            try {

                ASSERT(f.isLink());
                std::string contname = f.baseName();
                std::string uuid_str = f.realName().baseName();

                if (contname.rfind("__dummy_daos_uuid_", 0) != 0) {
                    const char* contname_cstr = contname.c_str();
                    ASSERT(strlen(contname_cstr) <= DAOS_PROP_LABEL_MAX_LEN);
                    strncpy(cbuf[nfound - 1].pci_label, contname_cstr, DAOS_PROP_LABEL_MAX_LEN + 1);
                }

                const char* uuid_cstr = uuid_str.c_str();
                uuid_t uuid = {0};
                ASSERT(uuid_parse(uuid_cstr, uuid) == 0);
                uuid_copy(cbuf[nfound - 1].pci_uuid, uuid);
            }
            catch (eckit::FailedSystemCall& e) {

                if (f.exists()) {
                    throw;
                }
                --nfound;
            }
        }
    }

    *ncont = nfound;

    return 0;
}

int daos_cont_create_internal(daos_handle_t poh, uuid_t* uuid) {

    ASSERT(poh.impl);

    /// @note: name generation copied from LocalPathName::unique. Ditched StaticMutex
    ///        as dummy DAOS is not thread safe.

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

    uuid_t new_uuid = {0};
    eckit::MD5 md5(name);
    uint64_t hi = std::stoull(md5.digest().substr(0, 8), nullptr, 16);
    uint64_t lo = std::stoull(md5.digest().substr(8, 16), nullptr, 16);
    ::memcpy(&new_uuid[0], &hi, sizeof(hi));
    ::memcpy(&new_uuid[8], &lo, sizeof(lo));

    char cont_uuid_cstr[37] = "";
    uuid_unparse(new_uuid, cont_uuid_cstr);

    eckit::PathName cont_path = poh.impl->path / cont_uuid_cstr;

    if (cont_path.exists()) {
        throw eckit::SeriousBug("UUID clash in cont create");
    }

    cont_path.mkdir();

    if (uuid != NULL) {
        uuid_copy(*uuid, new_uuid);
    }

    return 0;
}

/// @note: containers are implemented as directories within pool directories. Upon creation, a
///        container directory is named with a newly generated UUID. If a label is specified, a
///        symlink is created with the label as origin file name and the UUID as target directory.
///        If no label is specified, a similr symlink is created with the UUID with the
///        "__dummy_daos_uuid_" prefix as origin file name and the UUID as target directory.
///        This mechanism is necessary for listing and removing containers under concurrent,
///        potentially racing container operations.

int daos_cont_create(daos_handle_t poh, uuid_t* uuid, daos_prop_t* cont_prop, daos_event_t* ev) {

    ASSERT(poh.impl);

    if (cont_prop != NULL && cont_prop->dpp_entries) {

        if (cont_prop->dpp_nr != 1) {
            NOTIMP;
        }
        if (cont_prop->dpp_entries[0].dpe_type != DAOS_PROP_CO_LABEL) {
            NOTIMP;
        }

        struct daos_prop_entry* entry = &cont_prop->dpp_entries[0];

        if (entry == NULL) {
            NOTIMP;
        }

        std::string cont_name{entry->dpe_str};

        return daos_cont_create_with_label(poh, cont_name.c_str(), NULL, uuid, ev);
    }

    if (ev != NULL) {
        NOTIMP;
    }

    uuid_t new_uuid = {0};

    ASSERT(daos_cont_create_internal(poh, &new_uuid) == 0);

    char cont_uuid_cstr[37] = "";
    uuid_unparse(new_uuid, cont_uuid_cstr);

    eckit::PathName label_symlink_path = poh.impl->path / (std::string("__dummy_daos_uuid_") + cont_uuid_cstr);

    eckit::PathName cont_path = poh.impl->path / cont_uuid_cstr;

    if (::symlink(cont_path.path().c_str(), label_symlink_path.path().c_str()) < 0) {

        if (errno == EEXIST) {  // link path already exists due to race condition

            throw eckit::SeriousBug("unexpected race condition in unnamed container symlink creation");
        }
        else {  // symlink fails for unknown reason

            throw eckit::FailedSystemCall(std::string("symlink ") + cont_path.path() + " " + label_symlink_path.path());
        }
    }

    if (uuid != NULL) {
        uuid_copy(*uuid, new_uuid);
    }

    return 0;
}

int daos_cont_create_with_label(daos_handle_t poh, const char* label, daos_prop_t* cont_prop, uuid_t* uuid,
                                daos_event_t* ev) {

    ASSERT(poh.impl);
    if (cont_prop != NULL) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    ASSERT(std::string{label}.rfind("__dummy_daos_uuid_", 0) != 0);
    ASSERT(strlen(label) <= DAOS_PROP_LABEL_MAX_LEN);

    eckit::PathName label_symlink_path = poh.impl->path / label;

    if (label_symlink_path.exists()) {
        return 0;
    }

    uuid_t new_uuid = {0};

    ASSERT(daos_cont_create_internal(poh, &new_uuid) == 0);

    char cont_uuid_cstr[37] = "";
    uuid_unparse(new_uuid, cont_uuid_cstr);

    eckit::PathName cont_path = poh.impl->path / cont_uuid_cstr;

    if (::symlink(cont_path.path().c_str(), label_symlink_path.path().c_str()) < 0) {

        if (errno == EEXIST) {  // link path already exists due to race condition

            if (uuid != NULL) {
                /// @todo: again might find race condition here:
                std::string found_uuid = label_symlink_path.realName().baseName();
                uuid_parse(found_uuid.c_str(), *uuid);
            }

            deldir(cont_path);

            return 0;
        }
        else {  // symlink fails for unknown reason

            throw eckit::FailedSystemCall(std::string("symlink ") + cont_path.path() + " " + label_symlink_path.path());
        }
    }

    if (uuid != NULL) {
        uuid_copy(*uuid, new_uuid);
    }

    return 0;
}

/// @note: in DAOS, an attempt to destroy a container with open handles results
///        in error by default. This behavior is not implemented in dummy DAOS.
///        In DAOS, an attempt to destroy a container with open handles with the
///        force flag enabled closes open handles, and therefore ongoing/future
///        operations on these handles fail. The contained objects and the
///        container are immediately destroyed. In dummy DAOS the open handles
///        are implemented with file descriptors, and these are left open. A
///        remove operation is called on the corresponding file names but the
///        descriptors remain open. Therefore, in contrast to DAOS,
///        ongoing/future operations on these handles succeed, and the files
///        are destroyed after the descriptors are closed. The folder
///        implementing the container, however, is immediately removed.

int daos_cont_destroy(daos_handle_t poh, const char* cont, int force, daos_event_t* ev) {

    ASSERT(poh.impl);
    if (force != 1) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    ASSERT(std::string{cont}.rfind("__dummy_daos_uuid_", 0) != 0);

    eckit::PathName path = poh.impl->path;
    uuid_t uuid = {0};
    if (uuid_parse(cont, uuid) == 0) {
        path /= std::string("__dummy_daos_uuid_") + cont;
    }
    else {
        path /= cont;
    }

    eckit::PathName realpath("");
    try {

        ASSERT(path.isLink());
        realpath = path.realName();
        path.unlink();
    }
    catch (eckit::FailedSystemCall& e) {

        if (path.exists()) {
            throw;
        }
        return -DER_NONEXIST;
    }

    try {

        deldir(realpath);
    }
    catch (eckit::FailedSystemCall& e) {

        if (realpath.exists()) {
            throw;
        }
        return -DER_NONEXIST;
    }

    return 0;
}

int daos_cont_open(daos_handle_t poh, const char* cont, unsigned int flags, daos_handle_t* coh, daos_cont_info_t* info,
                   daos_event_t* ev) {

    ASSERT(poh.impl);
    if (flags != DAOS_COO_RW) {
        NOTIMP;
    }
    if (info != NULL) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    ASSERT(std::string{cont}.rfind("__dummy_daos_uuid_", 0) != 0);

    eckit::PathName path = poh.impl->path;
    uuid_t uuid = {0};
    if (uuid_parse(cont, uuid) == 0) {
        path /= std::string("__dummy_daos_uuid_") + cont;
    }
    else {
        path /= cont;
    }

    if (!path.exists()) {
        return -DER_NONEXIST;
    }

    eckit::PathName realpath{poh.impl->path};
    try {

        ASSERT(path.isLink());
        realpath /= path.realName().baseName();
    }
    catch (eckit::FailedSystemCall& e) {

        if (path.exists()) {
            throw;
        }
        return -DER_NONEXIST;
    }

    if (!realpath.exists()) {
        return -DER_NONEXIST;
    }

    auto impl = std::make_unique<daos_handle_internal_t>();
    impl->path = realpath;

    coh->impl = impl.release();
    return 0;
}

int daos_cont_close(daos_handle_t coh, daos_event_t* ev) {

    ASSERT(coh.impl);
    delete coh.impl;

    if (ev != NULL) {
        NOTIMP;
    }

    return 0;
}

int daos_cont_alloc_oids(daos_handle_t coh, daos_size_t num_oids, uint64_t* oid, daos_event_t* ev) {

    static uint64_t next_oid = 0;

    ASSERT(coh.impl);
    if (ev != NULL) {
        NOTIMP;
    }
    ASSERT(num_oids > (uint64_t)0);

    // support for multi-node clients running dummy DAOS backed by a
    // distributed file system
    std::string host = eckit::Main::instance().hostname();

    uuid_t uuid = {0};
    eckit::MD5 md5(host);
    uint64_t hi = std::stoull(md5.digest().substr(0, 8), nullptr, 16);
    uint64_t lo = std::stoull(md5.digest().substr(8, 16), nullptr, 16);
    ::memcpy(&uuid[0], &hi, sizeof(hi));
    ::memcpy(&uuid[8], &lo, sizeof(lo));

    char uuid_cstr[37] = "";
    uuid_unparse(uuid, uuid_cstr);

    pid_t pid = getpid();

    // only 20 out of the 32 bits in pid_t are used to identify the calling process.
    // because of this, there could be oid clashes
    uint64_t pid_mask = 0x00000000000FFFFF;

    uint64_t oid_mask = 0x000000000FFFFFFF;
    ASSERT((next_oid + num_oids) <= oid_mask);

    *oid = next_oid;
    *oid |= (((uint64_t)pid) & pid_mask) << 28;
    *oid |= (((uint64_t)*(((unsigned char*)uuid) + 1)) << 48);
    *oid |= (((uint64_t)*((unsigned char*)uuid)) << 56);

    next_oid += num_oids;

    return 0;
}

int daos_obj_generate_oid(daos_handle_t coh, daos_obj_id_t* oid, enum daos_otype_t type, daos_oclass_id_t cid,
                          daos_oclass_hints_t hints, uint32_t args) {

    ASSERT(coh.impl);
    if (type != DAOS_OT_KV_HASHED && type != DAOS_OT_ARRAY && type != DAOS_OT_ARRAY_BYTE) {
        NOTIMP;
    }
    if (cid != OC_S1) {
        NOTIMP;
    }
    if (hints != 0) {
        NOTIMP;
    }
    if (args != 0) {
        NOTIMP;
    }

    oid->hi &= (uint64_t)0x00000000FFFFFFFF;
    oid->hi |= ((((uint64_t)type) & OID_FMT_TYPE_MAX) << OID_FMT_TYPE_SHIFT);
    oid->hi |= ((((uint64_t)cid) & OID_FMT_CLASS_MAX) << OID_FMT_CLASS_SHIFT);

    return 0;
}

/// @note: real daos_kv_open does not involve RPC whereas dummy daos requires
///   interaction with the file system
int daos_kv_open(daos_handle_t coh, daos_obj_id_t oid, unsigned int mode, daos_handle_t* oh, daos_event_t* ev) {

    ASSERT(coh.impl);
    if (mode != DAOS_OO_RW) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    std::stringstream os;
    os << std::setw(16) << std::setfill('0') << std::hex << oid.hi;
    os << ".";
    os << std::setw(16) << std::setfill('0') << std::hex << oid.lo;

    auto impl = std::make_unique<daos_handle_internal_t>();
    impl->path = coh.impl->path / os.str();

    impl->path.mkdir();

    oh->impl = impl.release();
    return 0;
}

/// @note: destruction of KVs with open handles may not be consistent. Notes
///   in daos_cont_destroy apply here too.

int daos_kv_destroy(daos_handle_t oh, daos_handle_t th, daos_event_t* ev) {

    ASSERT(oh.impl);
    if (th.impl != DAOS_TX_NONE.impl) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    try {

        deldir(oh.impl->path);
    }
    catch (eckit::FailedSystemCall& e) {

        if (oh.impl->path.exists()) {
            throw;
        }
        return -DER_NONEXIST;
    }

    return 0;
}

int daos_obj_close(daos_handle_t oh, daos_event_t* ev) {

    ASSERT(oh.impl);
    delete oh.impl;

    if (ev != NULL) {
        NOTIMP;
    }

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

int daos_kv_put(daos_handle_t oh, daos_handle_t th, uint64_t flags, const char* key, daos_size_t size, const void* buf,
                daos_event_t* ev) {

    ASSERT(oh.impl);
    if (th.impl != DAOS_TX_NONE.impl) {
        NOTIMP;
    }
    if (flags != 0) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    eckit::FileHandle fh(oh.impl->path / key, true);
    fh.openForWrite(eckit::Length(size));
    eckit::AutoClose closer(fh);
    long res = fh.write(buf, (long)size);
    ASSERT(res == (long)size);

    return 0;
}

int daos_kv_get(daos_handle_t oh, daos_handle_t th, uint64_t flags, const char* key, daos_size_t* size, void* buf,
                daos_event_t* ev) {

    ASSERT(oh.impl);
    if (th.impl != DAOS_TX_NONE.impl) {
        NOTIMP;
    }
    if (flags != 0) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    bool exists = (oh.impl->path / key).exists();

    if (!exists && buf != NULL) {
        return -DER_NONEXIST;
    }

    daos_size_t dest_size = *size;
    *size = 0;
    if (!exists) {
        return 0;
    }

    eckit::FileHandle fh(oh.impl->path / key);
    eckit::Length len = fh.size();
    *size = len;

    if (buf == NULL) {
        return 0;
    }

    if (len > dest_size) {
        return -1;
    }

    fh.openForRead();
    eckit::AutoClose closer(fh);
    long res = fh.read(buf, len);
    ASSERT(eckit::Length(res) == len);

    return 0;
}

int daos_kv_remove(daos_handle_t oh, daos_handle_t th, uint64_t flags, const char* key, daos_event_t* ev) {

    ASSERT(oh.impl);
    if (th.impl != DAOS_TX_NONE.impl) {
        NOTIMP;
    }
    if (flags != 0) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    if (!oh.impl->path.exists()) {
        return -1;
    }

    /// @todo: should removal of a non-existing key fail?
    /// @todo: if not, can the exist check be avoided and unlink be called directly?
    if ((oh.impl->path / key).exists()) {
        (oh.impl->path / key).unlink();
    }

    return 0;
}

int daos_kv_list(daos_handle_t oh, daos_handle_t th, uint32_t* nr, daos_key_desc_t* kds, d_sg_list_t* sgl,
                 daos_anchor_t* anchor, daos_event_t* ev) {

    ASSERT(oh.impl);
    static std::vector<std::string> ongoing_req;
    static std::string req_hash;
    static unsigned long long n = (((unsigned long long)::getpid()) << 32);

    if (th.impl != DAOS_TX_NONE.impl) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    if (nr == NULL) {
        return -1;
    }
    if (kds == NULL) {
        return -1;
    }
    if (sgl == NULL) {
        return -1;
    }
    if (sgl->sg_nr != 1) {
        NOTIMP;
    }
    if (sgl->sg_iovs == NULL) {
        return -1;
    }
    if (anchor == NULL) {
        return -1;
    }

    if (!oh.impl->path.exists()) {
        return -1;
    }

    if (anchor->da_type == DAOS_ANCHOR_TYPE_EOF) {
        return -1;
    }
    if (anchor->da_type == DAOS_ANCHOR_TYPE_HKEY) {
        NOTIMP;
    }

    if (anchor->da_type == DAOS_ANCHOR_TYPE_ZERO) {

        /// client process must consume all key names before starting a new request
        if (ongoing_req.size() != 0) {
            NOTIMP;
        }

        std::vector<eckit::PathName> files;
        std::vector<eckit::PathName> dirs;
        oh.impl->path.children(files, dirs);

        for (auto& f : files) {
            ongoing_req.push_back(f.baseName());
        }

        anchor->da_type = DAOS_ANCHOR_TYPE_KEY;

        std::string hostname = eckit::Main::hostname();
        static std::string format = "%Y%m%d.%H%M%S";
        std::ostringstream os;
        os << eckit::TimeStamp(format) << '.' << hostname << '.' << n++;
        std::string name = os.str();
        while (::access(name.c_str(), F_OK) == 0) {
            std::ostringstream os;
            os << eckit::TimeStamp(format) << '.' << hostname << '.' << n++;
            name = os.str();
        }
        uuid_t new_uuid = {0};
        eckit::MD5 md5(name);
        req_hash = md5.digest();

        ::memcpy((char*)&(anchor->da_buf[0]), req_hash.c_str(), req_hash.size());
        anchor->da_shard = (uint16_t)req_hash.size();
    }
    else {

        if (anchor->da_type != DAOS_ANCHOR_TYPE_KEY) {
            throw eckit::SeriousBug("Unexpected anchor type");
        }

        /// different processes cannot collaborate on consuming a same kv_list
        /// request (i.e. cannot share a hash)
        if (std::string((char*)&(anchor->da_buf[0]), anchor->da_shard) != req_hash) {
            NOTIMP;
        }
    }

    size_t remain_size = sgl->sg_iovs[0].iov_buf_len;
    uint32_t remain_kds = *nr;
    size_t sgl_pos = 0;
    *nr = 0;
    while (remain_kds > 0 && remain_size > 0 && ongoing_req.size() > 0) {
        size_t next_size = ongoing_req.back().size();
        if (next_size > remain_size) {
            if (*nr == 0) {
                return -1;
            }
            remain_size = 0;
            continue;
        }
        ::memcpy((char*)sgl->sg_iovs[0].iov_buf + sgl_pos, ongoing_req.back().c_str(), next_size);
        ongoing_req.pop_back();
        kds[*nr].kd_key_len = next_size;
        remain_size -= next_size;
        remain_kds--;
        sgl_pos += next_size;
        *nr += 1;
    }

    if (ongoing_req.size() == 0) {
        anchor->da_type = DAOS_ANCHOR_TYPE_EOF;
    }

    return 0;
}

int daos_array_generate_oid(daos_handle_t coh, daos_obj_id_t* oid, bool add_attr, daos_oclass_id_t cid,
                            daos_oclass_hints_t hints, uint32_t args) {

    if (add_attr) {

        return daos_obj_generate_oid(coh, oid, DAOS_OT_ARRAY, cid, hints, args);
    }
    else {

        return daos_obj_generate_oid(coh, oid, DAOS_OT_ARRAY_BYTE, cid, hints, args);
    }
}

int daos_array_create(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th, daos_size_t cell_size,
                      daos_size_t chunk_size, daos_handle_t* oh, daos_event_t* ev) {

    ASSERT(coh.impl);
    if (th.impl != DAOS_TX_NONE.impl) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    std::stringstream os;
    os << std::setw(16) << std::setfill('0') << std::hex << oid.hi;
    os << ".";
    os << std::setw(16) << std::setfill('0') << std::hex << oid.lo;

    auto impl = std::make_unique<daos_handle_internal_t>();
    impl->path = coh.impl->path / os.str();

    impl->path.touch();

    oh->impl = impl.release();
    return 0;
}

int daos_array_destroy(daos_handle_t oh, daos_handle_t th, daos_event_t* ev) {

    ASSERT(oh.impl);
    if (th.impl != DAOS_TX_NONE.impl) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    try {

        oh.impl->path.unlink();
    }
    catch (eckit::FailedSystemCall& e) {

        if (oh.impl->path.exists()) {
            throw;
        }
        return -DER_NONEXIST;
    }

    return 0;
}

int daos_array_open(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th, unsigned int mode, daos_size_t* cell_size,
                    daos_size_t* chunk_size, daos_handle_t* oh, daos_event_t* ev) {

    ASSERT(coh.impl);
    if (th.impl != DAOS_TX_NONE.impl) {
        NOTIMP;
    }
    if (mode != DAOS_OO_RW) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    *cell_size = 1;
    *chunk_size = (uint64_t)1048576;

    std::stringstream os;
    os << std::setw(16) << std::setfill('0') << std::hex << oid.hi;
    os << ".";
    os << std::setw(16) << std::setfill('0') << std::hex << oid.lo;

    auto impl = std::make_unique<daos_handle_internal_t>();
    impl->path = coh.impl->path / os.str();

    if (!impl->path.exists()) {
        return -DER_NONEXIST;
    }

    oh->impl = impl.release();
    return 0;
}

int daos_array_open_with_attr(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th, unsigned int mode,
                              daos_size_t cell_size, daos_size_t chunk_size, daos_handle_t* oh, daos_event_t* ev) {

    ASSERT(coh.impl);
    if (th.impl != DAOS_TX_NONE.impl) {
        NOTIMP;
    }
    if (mode != DAOS_OO_RW) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    if (cell_size != 1) {
        NOTIMP;
    }
    if (chunk_size != (uint64_t)1048576) {
        NOTIMP;
    }

    return daos_array_create(coh, oid, th, cell_size, chunk_size, oh, ev);
}

int daos_array_close(daos_handle_t oh, daos_event_t* ev) {

    ASSERT(oh.impl);
    delete oh.impl;

    if (ev != NULL) {
        NOTIMP;
    }

    return 0;
}

// daos_array_write and read have the same limitations and transactional behavior as
// daos_kv_put and get

int daos_array_write(daos_handle_t oh, daos_handle_t th, daos_array_iod_t* iod, d_sg_list_t* sgl, daos_event_t* ev) {

    ASSERT(oh.impl);
    if (th.impl != DAOS_TX_NONE.impl) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    if (iod->arr_nr != 1) {
        NOTIMP;
    }

    if (sgl->sg_nr != 1) {
        NOTIMP;
    }
    // source memory len
    if (sgl->sg_iovs[0].iov_buf_len != sgl->sg_iovs[0].iov_len) {
        NOTIMP;
    }
    // source memory vs. target object len
    if (sgl->sg_iovs[0].iov_buf_len != iod->arr_rgs[0].rg_len) {
        NOTIMP;
    }

    // sgl->sg_iovs[0].iov_buf is a void * with the data to write
    // sgl->sg_iovs[0].iov_buf_len is a size_t with the source len
    // iod->arr_rgs[0].rg_idx is a uint64_t with the offset to write from

    eckit::FileHandle fh(oh.impl->path, true);

    // eckit::Length existing_len = fh.size();

    // if writing data to an already existing and populated file, if the data to write
    // is smaller than the file or the data has an offset, the holes will be left with
    // pre-existing data (openForAppend) rather than zero-d out (openForWrite)

    fh.openForAppend(eckit::Length(sgl->sg_iovs[0].iov_buf_len));
    eckit::AutoClose closer(fh);
    fh.seek(iod->arr_rgs[0].rg_idx);
    long res = fh.write(sgl->sg_iovs[0].iov_buf, (long)sgl->sg_iovs[0].iov_buf_len);
    ASSERT(res == (long)sgl->sg_iovs[0].iov_buf_len);

    return 0;
}

int daos_array_get_size(daos_handle_t oh, daos_handle_t th, daos_size_t* size, daos_event_t* ev) {

    ASSERT(oh.impl);
    if (th.impl != DAOS_TX_NONE.impl) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    *size = (daos_size_t)oh.impl->path.size();

    return 0;
}

int daos_array_read(daos_handle_t oh, daos_handle_t th, daos_array_iod_t* iod, d_sg_list_t* sgl, daos_event_t* ev) {

    ASSERT(oh.impl);
    if (th.impl != DAOS_TX_NONE.impl) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    if (iod->arr_nr != 1) {
        NOTIMP;
    }

    if (sgl->sg_nr != 1) {
        NOTIMP;
    }
    // target memory len
    if (sgl->sg_iovs[0].iov_buf_len != sgl->sg_iovs[0].iov_len) {
        NOTIMP;
    }
    // target memory vs. source object len
    if (sgl->sg_iovs[0].iov_buf_len != iod->arr_rgs[0].rg_len) {
        NOTIMP;
    }

    // sgl->sg_iovs[0].iov_buf is a void * where to read the data into
    // iod->arr_rgs[0].rg_len is a size_t with the source size
    // iod->arr_rgs[0].rg_idx is a uint64_t with the offset to read from

    eckit::FileHandle fh(oh.impl->path);
    eckit::Length len = fh.size();
    if (iod->arr_rgs[0].rg_len + iod->arr_rgs[0].rg_idx > len) {
        return -1;
    }
    fh.openForRead();
    eckit::AutoClose closer(fh);
    fh.seek(iod->arr_rgs[0].rg_idx);
    long res = fh.read(sgl->sg_iovs[0].iov_buf, iod->arr_rgs[0].rg_len);
    ASSERT(eckit::Length(res) == eckit::Length(iod->arr_rgs[0].rg_len));

    return 0;
}

int daos_cont_create_snap_opt(daos_handle_t coh, daos_epoch_t* epoch, char* name, enum daos_snapshot_opts opts,
                              daos_event_t* ev) {

    ASSERT(coh.impl);
    if (name != NULL) {
        NOTIMP;
    }
    if (opts != (DAOS_SNAP_OPT_CR | DAOS_SNAP_OPT_OIT)) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    std::vector<eckit::PathName> files;
    std::vector<eckit::PathName> dirs;

    coh.impl->path.children(files, dirs);

    std::string oids = "";
    std::string sep = "";

    for (std::vector<eckit::PathName>* fileset : {&files, &dirs}) {
        for (std::vector<eckit::PathName>::iterator it = fileset->begin(); it != fileset->end(); ++it) {

            std::string oid = it->baseName();

            if (strstr(oid.c_str(), ".snap")) {
                continue;
            }

            ASSERT(oid.length() == 33);

            oids += sep + oid;
            sep = ",";
        }
    }

    std::ostringstream os;
    os << eckit::TimeStamp("hex");
    std::string ts = os.str();
    ASSERT(ts.length() == 16);

    // if writing data to an already existing and populated file, if the data to write
    // is smaller than the file, the holes will be zero-d out (openForWrite) rather than
    // left with pre-existing data (openForAppend)

    eckit::FileHandle fh(coh.impl->path / ts + ".snap", true);
    fh.openForWrite(eckit::Length(oids.size()));
    {
        eckit::AutoClose closer(fh);
        fh.write(oids.data(), oids.size());
    }

    *epoch = std::stoull(ts, nullptr, 16);

    return 0;
}

int daos_cont_destroy_snap(daos_handle_t coh, daos_epoch_range_t epr, daos_event_t* ev) {

    ASSERT(coh.impl);
    if (epr.epr_hi != epr.epr_lo) {
        NOTIMP;
    }
    if (ev != NULL) {
        NOTIMP;
    }

    std::stringstream os;
    os << std::setw(16) << std::setfill('0') << std::hex << epr.epr_hi;

    eckit::PathName snap = coh.impl->path / os.str() + ".snap";

    if (!snap.exists()) {
        return -1;
    }

    snap.unlink();

    return 0;
}

int daos_oit_open(daos_handle_t coh, daos_epoch_t epoch, daos_handle_t* oh, daos_event_t* ev) {

    ASSERT(coh.impl);
    if (ev != NULL) {
        NOTIMP;
    }

    std::stringstream os;
    os << std::setw(16) << std::setfill('0') << std::hex << epoch;

    std::string ts = os.str();

    auto impl = std::make_unique<daos_handle_internal_t>();
    impl->path = coh.impl->path / ts + ".snap";

    if (!impl->path.exists()) {
        return -1;
    }

    oh->impl = impl.release();
    return 0;
}

int daos_oit_close(daos_handle_t oh, daos_event_t* ev) {

    ASSERT(oh.impl);
    delete oh.impl;

    if (ev != NULL) {
        NOTIMP;
    }

    return 0;
}

int daos_oit_list(daos_handle_t oh, daos_obj_id_t* oids, uint32_t* oids_nr, daos_anchor_t* anchor, daos_event_t* ev) {

    static std::vector<std::string> ongoing_req;
    static std::string req_hash;
    static unsigned long long n = (((unsigned long long)::getpid()) << 32);

    ASSERT(oh.impl);
    if (ev != NULL) {
        NOTIMP;
    }

    if (oids_nr == NULL) {
        return -1;
    }
    if (oids == NULL) {
        return -1;
    }
    if (anchor == NULL) {
        return -1;
    }

    if (!oh.impl->path.exists()) {
        return -1;
    }

    if (anchor->da_type == DAOS_ANCHOR_TYPE_EOF) {
        return -1;
    }
    if (anchor->da_type == DAOS_ANCHOR_TYPE_HKEY) {
        NOTIMP;
    }

    if (anchor->da_type == DAOS_ANCHOR_TYPE_ZERO) {

        /// client process must consume all key names before starting a new request
        if (ongoing_req.size() != 0) {
            NOTIMP;
        }

        eckit::Length size(oh.impl->path.size());
        std::vector<char> v(size);

        eckit::FileHandle fh(oh.impl->path, false);
        fh.openForRead();
        {
            eckit::AutoClose closer(fh);
            fh.read(&v[0], size);
        }

        std::string oids{v.begin(), v.end()};
        eckit::Tokenizer parse(",");
        parse(oids, ongoing_req);

        anchor->da_type = DAOS_ANCHOR_TYPE_KEY;

        std::string hostname = eckit::Main::hostname();
        static std::string format = "%Y%m%d.%H%M%S";
        std::ostringstream os;
        os << eckit::TimeStamp(format) << '.' << hostname << '.' << n++;
        std::string name = os.str();
        while (::access(name.c_str(), F_OK) == 0) {
            std::ostringstream os;
            os << eckit::TimeStamp(format) << '.' << hostname << '.' << n++;
            name = os.str();
        }
        uuid_t new_uuid = {0};
        eckit::MD5 md5(name);
        req_hash = md5.digest();

        ::memcpy((char*)&(anchor->da_buf[0]), req_hash.c_str(), req_hash.size());
        anchor->da_shard = (uint16_t)req_hash.size();
    }
    else {

        if (anchor->da_type != DAOS_ANCHOR_TYPE_KEY) {
            throw eckit::SeriousBug("Unexpected anchor type");
        }

        /// different processes cannot collaborate on consuming a same kv_list
        /// request (i.e. cannot share a hash)
        if (std::string((char*)&(anchor->da_buf[0]), anchor->da_shard) != req_hash) {
            NOTIMP;
        }
    }

    uint32_t remain_oids = *oids_nr;
    *oids_nr = 0;
    while (remain_oids > 0 && ongoing_req.size() > 0) {

        std::string next_oid = ongoing_req.back();

        oids[*oids_nr] = daos_obj_id_t{std::stoull(next_oid.substr(17, 16), nullptr, 16),
                                       std::stoull(next_oid.substr(0, 16), nullptr, 16)};

        ongoing_req.pop_back();

        remain_oids--;
        *oids_nr += 1;
    }

    if (ongoing_req.size() == 0) {
        anchor->da_type = DAOS_ANCHOR_TYPE_EOF;
    }

    return 0;
}

//----------------------------------------------------------------------------------------------------------------------

}  // extern "C"

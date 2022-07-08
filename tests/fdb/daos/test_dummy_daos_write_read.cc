/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <unistd.h>
#include <uuid/uuid.h>

#include "eckit/testing/Test.h"

#include "daos.h"
#include "dummy_daos.h"

using namespace eckit::testing;
using namespace eckit;


namespace fdb {
namespace test {

//----------------------------------------------------------------------------------------------------------------------

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
}

CASE( "dummy_daos_write_then_read" ) {

    std::string pool = "a";
    int rc;
    
    // create a dummy daos pool manually
    PathName root = dummy_daos_root();
    PathName pool_path = root / pool;
    pool_path.mkdir();

    daos_init();

    daos_handle_t poh;

    rc = daos_pool_connect(pool.c_str(), NULL, DAOS_PC_RW, &poh, NULL, NULL);
    EXPECT(rc == 0);
    EXPECT(dummy_daos_get_handle_path(poh) == pool_path);
    
    char cont_uuid_label[37] = "00000000-0000-0000-0000-000000000000";
    uuid_t cont_uuid;
    uuid_parse(cont_uuid_label, cont_uuid);
    rc = daos_cont_create(poh, cont_uuid, NULL, NULL);
    EXPECT(rc == 0);
    EXPECT((dummy_daos_get_handle_path(poh) / cont_uuid_label).exists());

    daos_handle_t coh;

    rc = daos_cont_open(poh, cont_uuid, DAOS_COO_RW, &coh, NULL, NULL);
    EXPECT(rc == 0);
    EXPECT(dummy_daos_get_handle_path(coh) == pool_path / cont_uuid_label);

    std::string cont = "b";

    rc = daos_cont_create_with_label(poh, cont.c_str(), NULL, NULL, NULL);
    EXPECT(rc == 0);
    EXPECT((dummy_daos_get_handle_path(poh) / cont).exists());

    rc = daos_cont_open(poh, cont.c_str(), DAOS_COO_RW, &coh, NULL, NULL);
    EXPECT(rc == 0);
    EXPECT(dummy_daos_get_handle_path(coh) == pool_path / cont);

    daos_size_t size;
    daos_size_t oid_alloc_size = 1;

    // Key-Value

    daos_obj_id_t oid_kv;
    rc = daos_cont_alloc_oids(coh, oid_alloc_size, &oid_kv.lo, NULL);
    EXPECT(rc == 0);
    //EXPECT(oid_kv.lo == ((((uint64_t) getpid()) << 48) | (uint64_t) 0));

    rc = daos_obj_generate_oid(coh, &oid_kv, DAOS_OT_KV_HASHED, OC_S1, 0, 0);
    EXPECT(rc == 0);

    daos_handle_t oh_kv;
    rc = daos_kv_open(coh, oid_kv, DAOS_OO_RW, &oh_kv, NULL);
    EXPECT(rc == 0);
    std::stringstream os_kv;
    os_kv << std::setw(16) << std::setfill('0') << std::hex << oid_kv.hi;
    os_kv << ".";
    os_kv << std::setw(16) << std::setfill('0') << std::hex << oid_kv.lo;
    EXPECT((dummy_daos_get_handle_path(coh) / os_kv.str()).exists());

    std::string key = "key";
    std::string value = "value";

    rc = daos_kv_get(oh_kv, DAOS_TX_NONE, 0, key.c_str(), &size, NULL, NULL);
    EXPECT(rc == 0);
    EXPECT(size == (daos_size_t) 0);

    rc = daos_kv_put(oh_kv, DAOS_TX_NONE, 0, key.c_str(), std::strlen(value.c_str()), value.c_str(), NULL);
    EXPECT(rc == 0);
    EXPECT((dummy_daos_get_handle_path(oh_kv) / key).exists());

    char kv_get_buf[100] = "";
    rc = daos_kv_get(oh_kv, DAOS_TX_NONE, 0, key.c_str(), &size, NULL, NULL);
    EXPECT(rc == 0);
    EXPECT(size == (daos_size_t) std::strlen(value.c_str()));
    rc = daos_kv_get(oh_kv, DAOS_TX_NONE, 0, key.c_str(), &size, kv_get_buf, NULL);
    EXPECT(rc == 0);
    EXPECT(size == (daos_size_t) std::strlen(value.c_str()));
    EXPECT(std::strlen(kv_get_buf) == std::strlen(value.c_str()));
    EXPECT(std::string(kv_get_buf) == value);

    daos_obj_close(oh_kv, NULL);
    EXPECT(rc == 0);

    // Array write

    daos_obj_id_t oid;
    rc = daos_cont_alloc_oids(coh, oid_alloc_size, &oid.lo, NULL);
    EXPECT(rc == 0);
    //EXPECT(oid.lo == ((((uint64_t) getpid()) << 48) | (uint64_t) 1));

    rc = daos_array_generate_oid(coh, &oid, true, OC_S1, 0, 0);
    EXPECT(rc == 0);

    daos_handle_t oh;
    rc = daos_array_create(coh, oid, DAOS_TX_NONE, 1, 1048576, &oh, NULL);
    EXPECT(rc == 0);
    std::stringstream os;
    os << std::setw(16) << std::setfill('0') << std::hex << oid.hi;
    os << ".";
    os << std::setw(16) << std::setfill('0') << std::hex << oid.lo;
    EXPECT((dummy_daos_get_handle_path(coh) / os.str()).exists());

    char data[] = "test";

    daos_array_iod_t iod;
    daos_range_t rg;

    d_sg_list_t sgl;
    d_iov_t iov;

    iod.arr_nr = 1;
    rg.rg_len = (daos_size_t) sizeof(data);
    rg.rg_idx = (daos_off_t) 0;
    iod.arr_rgs = &rg;

    sgl.sg_nr = 1;
    d_iov_set(&iov, data, (size_t) sizeof(data));
    sgl.sg_iovs = &iov;

    rc = daos_array_write(oh, DAOS_TX_NONE, &iod, &sgl, NULL);
    EXPECT(rc == 0);
    EXPECT(dummy_daos_get_handle_path(oh).exists());

    daos_array_close(oh, NULL);
    EXPECT(rc == 0);

    // Array read

    daos_size_t cell_size, csize;
    rc = daos_array_open(coh, oid, DAOS_TX_NONE, DAOS_OO_RW, &cell_size, &csize, &oh, NULL);
    EXPECT(rc == 0);

    char *data_read;

    daos_size_t array_size;
    rc = daos_array_get_size(oh, DAOS_TX_NONE, &array_size, NULL);
    EXPECT(rc == 0);
    EXPECT(array_size == rg.rg_len);

    data_read = (char *) malloc(sizeof(char) * ((size_t) array_size));

    iod.arr_nr = 1;
    rg.rg_len = array_size;
    rg.rg_idx = (daos_off_t) 0;
    iod.arr_rgs = &rg;

    sgl.sg_nr = 1;
    d_iov_set(&iov, data_read, (size_t) array_size);
    sgl.sg_iovs = &iov;

    rc = daos_array_read(oh, DAOS_TX_NONE, &iod, &sgl, NULL);
    EXPECT(rc == 0);
    EXPECT(std::memcmp(data, data_read, sizeof(data)) == 0);

    free(data_read);

    rc = daos_array_close(oh, NULL);
    EXPECT(rc == 0);

    rc = daos_cont_close(coh, NULL);
    EXPECT(rc == 0);

    rc = daos_pool_disconnect(poh, NULL);
    EXPECT(rc == 0);

    daos_fini();

    // destroy the container and pool manually
    deldir(pool_path);
    
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <string>
#include <dirent.h>
#include <cstdio>

#include "eckit/filesystem/PathName.h"
#include "eckit/runtime/Main.h"
#include "eckit/io/Buffer.h"

#include "pmem/PersistentPtr.h"
#include "pmem/AtomicConstructor.h"

#include "fdb5/pmem/PRoot.h"
#include "fdb5/pmem/PIndexRoot.h"
#include "fdb5/pmem/DataPoolManager.h"

#include "test_persistent_helpers.h"

#include "eckit/testing/Test.h"

using namespace fdb5::pmem;
using namespace eckit;
using namespace eckit::testing;
using namespace pmem;

namespace fdb {
namespace test {

//----------------------------------------------------------------------------------------------------------------------

struct UniquePoolDir {
    UniquePoolDir(const pmem::AtomicConstructorBase& ctr) :
        path_(UniquePool().path_),
        basePoolPath_((path_.mkdir(), path_ / "master")),
        basePool_(basePoolPath_, 1025 * 1024 * 20, "pool-name", ctr) {

        /// Use a non-standard size (which we can test...)
        /// Minimum size is 8Mb. Use 11Mb
        putenv(const_cast<char*>("fdbPMemDataPoolSize=11534336"));
    }

    ~UniquePoolDir() {
        // Clean up all the files

        std::vector<PathName> files;
        std::vector<PathName> dirs;
        path_.children(files, dirs);

        EXPECT(dirs.size() == 0);

        for (std::vector<PathName>::const_iterator fl = files.begin(); fl != files.end(); ++fl)
            fl->unlink();

        path_.rmdir();
    }

    eckit::PathName path_;
    eckit::PathName basePoolPath_;
    pmem::PersistentPool basePool_;
};

//----------------------------------------------------------------------------------------------------------------------

class MgrSpy : public DataPoolManager {
public:
    DataPool& currentWritePool() { return DataPoolManager::currentWritePool(); }
    void invalidateCurrentPool(DataPool& p) { DataPoolManager::invalidateCurrentPool(p); }
    const std::map<uint64_t, DataPool*>& pools() const { return DataPoolManager::pools(); }
};

//----------------------------------------------------------------------------------------------------------------------

CASE( "test_fdb5_pmem_pool_manager_schema_create_pool" )
{
    UniquePoolDir up((PRoot::Constructor(PRoot::IndexClass)));
    up.basePool_.getRoot<PRoot>()->buildRoot(fdb5::Key(), "");

    PIndexRoot& ir(up.basePool_.getRoot<PRoot>()->indexRoot());

    {
        DataPoolManager mgr(up.path_, ir, 12345);

        // Load up the schema from where it is installed in the test system, and then
        // check that this equals whatever the DataPoolManager has stored.

        EXPECT(ir.dataPoolUUIDs().size() == size_t(0));

        DataPool& pool(static_cast<MgrSpy*>(&mgr)->currentWritePool());
        EXPECT(ir.dataPoolUUIDs().size() == size_t(1));

        // Check that the size of the pool is correctly read
        EXPECT(pool.size() == size_t(11534336));

        EXPECT(pool.path().baseName() == "data-0");

        // We can keep accessing the current write pool until it is invalidated (full)

        DataPool& pool2(static_cast<MgrSpy*>(&mgr)->currentWritePool());
        EXPECT(&pool == &pool2);
        EXPECT(pool.uuid() == pool2.uuid());
        EXPECT(ir.dataPoolUUIDs().size() == size_t(1));

        // But if the pool is invalidated, we get another one!

        static_cast<MgrSpy*>(&mgr)->invalidateCurrentPool(pool);
       /// DataPool& pool3(static_cast<MgrSpy*>(&mgr)->currentWritePool());
       /// BOOST_CHECK_EQUAL(ir.dataPoolUUIDs().size(), size_t(2));
       /// BOOST_CHECK(&pool != &pool3);
       /// BOOST_CHECK(pool.uuid() != pool3.uuid());

       /// BOOST_CHECK_EQUAL(pool3.path().baseName(), "data-1");
    }
}

CASE( "test_fdb5_pmem_pool_manager_print" )
{
    UniquePoolDir up((PRoot::Constructor(PRoot::IndexClass)));
    up.basePool_.getRoot<PRoot>()->buildRoot(fdb5::Key(), "");
    PIndexRoot& ir(up.basePool_.getRoot<PRoot>()->indexRoot());

    {
        DataPoolManager mgr(up.path_, ir, 12345);

        std::stringstream ss;
        ss << mgr;

        std::string correct = std::string("PMemDataPoolManager(") + std::string(up.path_) + ")";
        EXPECT(ss.str() == correct);
    }
}

CASE( "test_fdb5_pmem_pool_manager_pool_reopen" )
{
    UniquePoolDir up((PRoot::Constructor(PRoot::IndexClass)));
    up.basePool_.getRoot<PRoot>()->buildRoot(fdb5::Key(), "");
    PIndexRoot& ir(up.basePool_.getRoot<PRoot>()->indexRoot());
    uint64_t uuid = 0;

    {
        DataPoolManager mgr(up.path_, ir, 12345);

        DataPool& pool(static_cast<MgrSpy*>(&mgr)->currentWritePool());
        uuid = pool.uuid();

        EXPECT(!pool.finalised());
    }

    // Re-open the same pool. Check that when we go to write, then we get back the same pool
    // that we created before.

    {
        DataPoolManager mgr(up.path_, ir, 12345);

        DataPool& pool(static_cast<MgrSpy*>(&mgr)->currentWritePool());

        EXPECT(uuid == pool.uuid());
        EXPECT(!pool.finalised());

        // Check that the size of the pool is correctly read
        EXPECT(pool.size() == size_t(11534336));
    }
}


CASE( "test_fdb5_pmem_pool_manager_pool_invalidation" )
{
    UniquePoolDir up((PRoot::Constructor(PRoot::IndexClass)));
    up.basePool_.getRoot<PRoot>()->buildRoot(fdb5::Key(), "");
    PIndexRoot& ir(up.basePool_.getRoot<PRoot>()->indexRoot());
    uint64_t uuid = 0;

    {
        DataPoolManager mgr(up.path_, ir, 12345);

        // n.b. Finilising a pool does NOT notify this to any other processes that have this pool
        //      open for writing. They are free to (attempt) to write to the pool (and likely
        //      fail and roll over).
        //
        //      All that marking this as finalised does is ensure that on re-opening a pool, we
        //      don't attempt to re-open that one.

        DataPool& pool(static_cast<MgrSpy*>(&mgr)->currentWritePool());
        uuid = pool.uuid();

        static_cast<MgrSpy*>(&mgr)->invalidateCurrentPool(pool);

        EXPECT(pool.finalised());
    }

    // Re-open the same pool. Check that when we request the currentWritePool, it doesn't give us
    // the finalised pool.

    {
        DataPoolManager mgr(up.path_, ir, 12345);

        DataPool& pool(static_cast<MgrSpy*>(&mgr)->currentWritePool());

        EXPECT(uuid != pool.uuid());
        EXPECT(!pool.finalised());
    }
}


CASE( "test_fdb5_pmem_pool_manager_pool_load_pool" )
{
    UniquePoolDir up((PRoot::Constructor(PRoot::IndexClass)));
    up.basePool_.getRoot<PRoot>()->buildRoot(fdb5::Key(), "");
    PIndexRoot& ir(up.basePool_.getRoot<PRoot>()->indexRoot());
    uint64_t uuid = 0;

    {
        DataPoolManager mgr(up.path_, ir, 12345);

        // n.b. Finilising a pool does NOT notify this to any other processes that have this pool
        //      open for writing. They are free to (attempt) to write to the pool (and likely
        //      fail and roll over).
        //
        //      All that marking this as finalised does is ensure that on re-opening a pool, we
        //      don't attempt to re-open that one.

        DataPool& pool(static_cast<MgrSpy*>(&mgr)->currentWritePool());
        uuid = pool.uuid();
    }

    // Reopen the pool manager, and make it load the requested pool.

    {
        DataPoolManager mgr(up.path_, ir, 12345);

        EXPECT(static_cast<MgrSpy*>(&mgr)->pools().size() == size_t(0));

        mgr.ensurePoolLoaded(uuid);

        EXPECT(static_cast<MgrSpy*>(&mgr)->pools().size() == size_t(1));
        EXPECT(static_cast<MgrSpy*>(&mgr)->pools().find(uuid) != static_cast<MgrSpy*>(&mgr)->pools().end());
    }
}

CASE( "test_fdb5_pmem_pool_manager_pool_allocate" )
{
    // Perform two allocations, one which fits and the other which will overflow the buffer.

    UniquePoolDir up((PRoot::Constructor(PRoot::IndexClass)));
    up.basePool_.getRoot<PRoot>()->buildRoot(fdb5::Key(), "");
    PIndexRoot& ir(up.basePool_.getRoot<PRoot>()->indexRoot());

    eckit::Buffer scratch_buf(6 * 1024 * 1024);

    {
        DataPoolManager mgr(up.path_, ir, 12345);

        DataPool& pool1(static_cast<MgrSpy*>(&mgr)->currentWritePool());
        uint64_t uuid1 = pool1.uuid();

        PersistentPtr<PersistentBuffer> tmp1;
        mgr.allocate(tmp1, AtomicConstructor2<PersistentBuffer, const void*, size_t>(
                         static_cast<const char*>(scratch_buf), scratch_buf.size()));

        DataPool& pool2(static_cast<MgrSpy*>(&mgr)->currentWritePool());
        uint64_t uuid2 = pool2.uuid();

        EXPECT(uuid1 == uuid2);

        PersistentPtr<PersistentBuffer> tmp2;
        mgr.allocate(tmp2, AtomicConstructor2<PersistentBuffer, const void*, size_t>(
                         static_cast<const char*>(scratch_buf), scratch_buf.size()));

        DataPool& pool3(static_cast<MgrSpy*>(&mgr)->currentWritePool());
        uint64_t uuid3 = pool3.uuid();

        EXPECT(uuid1 != uuid3);
    }
}

// TODO: Test allocate()

//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    eckit::Main::initialise(argc, argv, "FDB_HOME");
    return run_tests ( argc, argv, false );
}

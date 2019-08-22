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
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include <string>

#include "eckit/thread/Thread.h"
#include "eckit/thread/ThreadControler.h"

#include "pmem/PersistentPtr.h"

#include "fdb5/pmem/PBranchingNode.h"
#include "fdb5/pmem/PDataNode.h"
#include "fdb5/pmem/DataPoolManager.h"
#include "fdb5/database/Key.h"

#include "test_persistent_helpers.h"
#include "eckit/testing/Test.h"

using namespace eckit::testing;
using namespace fdb5::pmem;

namespace fdb {
namespace test {

//----------------------------------------------------------------------------------------------------------------------

class BrSpy : public PBranchingNode {
public:
    const pmem::PersistentVector<PBaseNode>& nodes() const { return nodes_; }
    const pmem::PersistentPtr<pmem::PersistentBuffer>& axis() const { return axis_; }
};

//----------------------------------------------------------------------------------------------------------------------

/// Define a root type. Each test that does allocation should use a different element in the root object.

const size_t root_elems = 6;

class RootType {

public: // constructor

    class Constructor : public pmem::AtomicConstructor<RootType> {
        virtual void make(RootType &object) const {
            for (size_t i = 0; i < root_elems; i++) {
                object.data_[i].nullify();
            }
        }
    };

public: // members

    pmem::PersistentPtr<PBranchingNode> data_[root_elems];
};

//----------------------------------------------------------------------------------------------------------------------

// Only need type_id for RootType. Others defined in Pool.cc

template<> uint64_t pmem::PersistentType<RootType>::type_id = POBJ_ROOT_TYPE_NUM;

// Create a global fixture, so that this pool is only created once, and destroyed once.

pmem::PersistentPtr<RootType> global_root;
pmem::PersistentPool* global_pool;

struct SuitePoolFixture {

    SuitePoolFixture() : autoPool_(RootType::Constructor()) {
        global_root = autoPool_.pool_.getRoot<RootType>();
        global_pool = &autoPool_.pool_;

        for (size_t i = 0; i < root_elems; i++) {
            global_root->data_[i].allocate("", "");
        }
    }
    ~SuitePoolFixture() {
        global_root.nullify();
        global_pool = 0;
    }

    AutoPool autoPool_;
};

SuitePoolFixture global_fixture;

//----------------------------------------------------------------------------------------------------------------------

CASE( "test_fdb5_pmem_pbranchingnode_basenode_functionality" )
{
    std::vector<char> elem(sizeof(PBranchingNode));

    PBranchingNode& dn = *new (&elem[0]) PBranchingNode("key1", "value1");

    EXPECT(!dn.isDataNode());
    EXPECT(!dn.isNull());
    EXPECT(dn.isBranchingNode());

    EXPECT(dn.matches("key1", "value1"));
    EXPECT(!dn.matches("key1", "value2"));
    EXPECT(!dn.matches("key2", "value1"));
}


CASE( "test_fdb5_pmem_pbranchingnode_trivial_constructor" )
{
    std::vector<char> elem(sizeof(PBranchingNode));

    PBranchingNode& dn = *new (&elem[0]) PBranchingNode("key1", "value1");

    EXPECT(static_cast<BrSpy*>(&dn)->nodes().size() == size_t(0));
    EXPECT(static_cast<BrSpy*>(&dn)->axis().null());
}


CASE( "test_fdb5_pmem_pbranchingnode_insert_mismatched_key" )
{
    // If a data node is added whose key-value pair doesn't match that supplied to the
    // branching node, then we should throw a wobbly.

    PBranchingNode& root(*global_root->data_[0]);
    DataPoolManager mgrMock("", *reinterpret_cast<PIndexRoot*>(0), global_root.uuid());

    // Allocate a data node, and then insert it into the tree

    fdb5::Key key;
    key.push("key1", "value1");
    key.push("key2", "value2");

    std::string data1 = "This is some data. Wooooooooooooooooo!";

    pmem::PersistentPtr<PDataNode> ptr;
    ptr.allocate_ctr(*global_pool, PDataNode::Constructor("keyz", "valuez", data1.data(), data1.size()));

    EXPECT_THROWS_AS(root.insertDataNode(key, ptr, mgrMock), eckit::AssertionFailed);
}


CASE( "test_fdb5_pmem_pbranchingnode_insert_retrieve" )
{
    // Test that we can insert/retrieve with variously branching keys

    // Global objects. The Manager won't do anything, as everything is within the pool pointed to
    // by the global_root.uuid(), so the NULL reference will NEVER be used.
    PBranchingNode& root(*global_root->data_[1]);
    DataPoolManager mgrMock("", *reinterpret_cast<PIndexRoot*>(0), global_root.uuid());

    // --

    fdb5::Key key1;
    key1.push("key1", "value1");
    key1.push("key2", "value2");

    std::string data1 = "This is some data. Wooooooooooooooooo!";

    pmem::PersistentPtr<PDataNode> ptr1;
    ptr1.allocate_ctr(*global_pool, PDataNode::Constructor("key2", "value2", data1.data(), data1.size()));
    root.insertDataNode(key1, ptr1, mgrMock);

    // --

    fdb5::Key key2;
    key2.push("key1", "value1");
    key2.push("key2a", "value2a");

    std::string data2 = "Another bit of data";

    pmem::PersistentPtr<PDataNode> ptr2;
    ptr2.allocate_ctr(*global_pool, PDataNode::Constructor("key2a", "value2a", data2.data(), data2.size()));
    root.insertDataNode(key2, ptr2, mgrMock);


    // --

    fdb5::Key key3;
    key3.push("key1a", "value1a");
    key3.push("key2", "value2");

    std::string data3 = "And again...";

    pmem::PersistentPtr<PDataNode> ptr3;
    ptr3.allocate_ctr(*global_pool, PDataNode::Constructor("key2", "value2", data3.data(), data3.size()));
    root.insertDataNode(key3, ptr3, mgrMock);

    // --

    pmem::PersistentPtr<PDataNode> pdata1 = root.getDataNode(key1, mgrMock);
    pmem::PersistentPtr<PDataNode> pdata2 = root.getDataNode(key2, mgrMock);
    pmem::PersistentPtr<PDataNode> pdata3 = root.getDataNode(key3, mgrMock);

    EXPECT(ptr1 == pdata1);
    EXPECT(ptr2 == pdata2);
    EXPECT(ptr3 == pdata3);

    EXPECT(std::string(static_cast<const char*>(pdata1->data()), pdata1->length()) == data1);
    EXPECT(std::string(static_cast<const char*>(pdata2->data()), pdata2->length()) == data2);
    EXPECT(std::string(static_cast<const char*>(pdata3->data()), pdata3->length()) == data3);
}


CASE( "test_fdb5_pmem_pbranchingnode_insert_masking" )
{
    // If data is repeatedly added with the same key, it masks existing data (which is not overwritten).

    // Global objects. The Manager won't do anything, as everything is within the pool pointed to
    // by the global_root.uuid(), so the NULL reference will NEVER be used.
    PBranchingNode& root(*global_root->data_[2]);
    DataPoolManager mgrMock("", *reinterpret_cast<PIndexRoot*>(0), global_root.uuid());

    // Allocate a data node, and then insert it into the tree

    fdb5::Key key;
    key.push("key1", "value1");
    key.push("key2", "value2");

    std::string data1 = "This is some data. Wooooooooooooooooo!";

    pmem::PersistentPtr<PDataNode> ptr;
    ptr.allocate_ctr(*global_pool, PDataNode::Constructor("key2", "value2", data1.data(), data1.size()));

    root.insertDataNode(key, ptr, mgrMock);

    // Add another data node with the same key

    std::string data2 = "other data";

    pmem::PersistentPtr<PDataNode> ptr2;
    ptr2.allocate_ctr(*global_pool, PDataNode::Constructor("key2", "value2", data2.data(), data2.size()));

    root.insertDataNode(key, ptr2, mgrMock);

    // Test that the second one is the one which is made accessible by the API

    pmem::PersistentPtr<PDataNode> pdata = root.getDataNode(key, mgrMock);
    EXPECT(pdata == ptr2);

    std::string check_str(static_cast<const char*>(pdata->data()), pdata->length());
    EXPECT(check_str == data2);

    // Test that the original data is still there.

    EXPECT(static_cast<BrSpy*>(&root)->nodes().size() == size_t(1));
    EXPECT(static_cast<BrSpy*>(
                                &static_cast<BrSpy*>(&root)->nodes()[0]->asBranchingNode()
                              )->nodes().size() == size_t(2));
    pmem::PersistentPtr<PDataNode> pdata2 = static_cast<BrSpy*>(
                                               &static_cast<BrSpy*>(&root)->nodes()[0]->asBranchingNode()
                                            )->nodes()[0].as<PDataNode>();
    EXPECT(pdata2 == ptr);

    std::string check_str2(static_cast<const char*>(pdata2->data()), pdata2->length());
    EXPECT(check_str2 == data1);
}


CASE( "test_fdb5_pmem_pbranchingnode_insert_collision" )
{
    // If we insert a data node that would mask a branching node, then something has gone
    // wrong. We should hit an assertion.

    PBranchingNode& root(*global_root->data_[3]);
    DataPoolManager mgrMock("", *reinterpret_cast<PIndexRoot*>(0), global_root.uuid());

    // --

    fdb5::Key key1;
    key1.push("key1", "value1");
    key1.push("key2", "value2");
    key1.push("key3", "value3");

    std::string data1 = "This is some data. Wooooooooooooooooo!";

    pmem::PersistentPtr<PDataNode> ptr1;
    ptr1.allocate_ctr(*global_pool, PDataNode::Constructor("key3", "value3", data1.data(), data1.size()));
    root.insertDataNode(key1, ptr1, mgrMock);

    // --

    fdb5::Key key2;
    key2.push("key1", "value1");
    key2.push("key2", "value2");

    std::string data2 = "Another bit of data";

    pmem::PersistentPtr<PDataNode> ptr2;
    ptr2.allocate_ctr(*global_pool, PDataNode::Constructor("key2", "value2", data2.data(), data2.size()));

    EXPECT_THROWS_AS(root.insertDataNode(key2, ptr2, mgrMock), eckit::AssertionFailed);
}


CASE( "test_fdb5_pmem_pbranchingnode_find_fail" )
{
    // If we attempt to find a non-existent data node (or worse, one that is present but not
    // a data node) then fail gracefully.

    // Global objects. The Manager won't do anything, as everything is within the pool pointed to
    // by the global_root.uuid(), so the NULL reference will NEVER be used.
    PBranchingNode& root(*global_root->data_[4]);
    DataPoolManager mgrMock("", *reinterpret_cast<PIndexRoot*>(0), global_root.uuid());

    // --

    fdb5::Key key1;
    key1.push("key1", "value1");
    key1.push("key2", "value2");
    key1.push("key3", "value3");

    std::string data1 = "This is some data. Wooooooooooooooooo!";

    pmem::PersistentPtr<PDataNode> ptr1;
    ptr1.allocate_ctr(*global_pool, PDataNode::Constructor("key3", "value3", data1.data(), data1.size()));
    root.insertDataNode(key1, ptr1, mgrMock);

    // -- Finding a non-existent node

    fdb5::Key keyA;
    keyA.push("a_key", "a_value");
    keyA.push("anotherK", "anotherV");

    pmem::PersistentPtr<PDataNode> pdata1 = root.getDataNode(keyA, mgrMock);
    EXPECT(pdata1.null());

    // -- What if the searched for value isn't a DataNode. This is breaking the (implicit) schema
    //    so it throws.

    fdb5::Key keyB;
    keyB.push("key1", "value1");
    keyB.push("key2", "value2");

    EXPECT_THROWS_AS(root.getDataNode(keyB, mgrMock), eckit::AssertionFailed);
}


CASE( "test_fdb5_pmem_pbranchingnode_thread_testing" )
{
    // Try and test the thread safety by throwing LOTS of data at the tree from multiple threads.

    // Global objects. The Manager won't do anything, as everything is within the pool pointed to
    // by the global_root.uuid(), so the NULL reference will NEVER be used.
    PBranchingNode& root(*global_root->data_[5]);
    DataPoolManager mgrMock("", *reinterpret_cast<PIndexRoot*>(0), global_root.uuid());

    // --------------------

    class WriterThread : public eckit::Thread {
    public:
        WriterThread(const std::vector<std::pair<fdb5::Key, std::string> >& keys, PBranchingNode& root) :
            keys_(keys), root_(root),
            mgrMock_("", *reinterpret_cast<PIndexRoot*>(0), global_root.uuid()) {}
        virtual ~WriterThread() {}

        virtual void run() {

            // Iterate over the unique key descriptors passed in

            for (std::vector<std::pair<fdb5::Key, std::string> >::const_iterator it = keys_.begin();
                 it != keys_.end(); it++) {

                std::string datakey = it->first.names()[it->first.names().size()-1];
                std::string datavalue = it->first.value(datakey);
                std::string data_str = it->second;

                pmem::PersistentPtr<PDataNode> pdata;
                pdata.allocate_ctr(*global_pool, PDataNode::Constructor(datakey, datavalue, data_str.data(), data_str.length()));

                // And insert into the tree

                root_.insertDataNode(it->first, pdata, mgrMock_);
            }
        }

    private:
        const std::vector<std::pair<fdb5::Key, std::string> >& keys_;
        PBranchingNode& root_;
        DataPoolManager mgrMock_;
    };

    // --------------------

    class Threads {

    public: // methods

        Threads() {}
        ~Threads() {
            for (std::vector<eckit::ThreadControler*>::iterator it = threads_.begin(); it != threads_.end(); ++it)
                delete *it;
        }

        void push(eckit::Thread* t) { threads_.push_back(new eckit::ThreadControler(t, false)); }

        void go() {
            for (std::vector<eckit::ThreadControler*>::iterator it = threads_.begin(); it != threads_.end(); ++it)
                (*it)->start();
            for (std::vector<eckit::ThreadControler*>::iterator it = threads_.begin(); it != threads_.end(); ++it)
                (*it)->wait();
        }

    private: // members

        std::vector<eckit::ThreadControler*> threads_;
    };

    // --------------------

    // Generate a list of sets of four unique indices. These will determine what each of the threads
    // is going to add.

    const size_t keys_per_thread = 10;
    size_t thread_key_count = 0;
    std::vector<std::vector<std::pair<fdb5::Key, std::string> > > keys;
    std::vector<std::pair<fdb5::Key, std::string> > part_key;

    std::vector<int> ks(4, 0);
    std::vector<int> k_max(4);
    k_max[0] = 4;
    k_max[1] = 5;
    k_max[2] = 6;
    k_max[3] = 7;

    bool found;
    do {
        found = false;
        for (size_t pos = 0; pos < 4; pos++) {
            if (ks[pos] < k_max[pos]) {
                ks[pos]++;
                for (size_t pos2 = 0; pos2 < pos; pos2++)
                    ks[pos2] = 0;

                // Generate a unique key and data pair

                fdb5::Key insert_key;
                std::stringstream data_ss;
                for (size_t i = 0; i < ks.size(); i++) {
                    std::stringstream keybit, valuebit;
                    keybit << "key" << i;
                    valuebit << "value" << ks[i];
                    data_ss << i << ks[i];
                    insert_key.push(keybit.str(), valuebit.str());
                }

                // Add it to the list
                part_key.push_back(std::make_pair(insert_key, data_ss.str()));

                // Move onto the next one, as appropriate.
                // (don't worry about the small overflow at the end, where things aren't nice
                //  round multiples).

                thread_key_count++;
                if (thread_key_count >= keys_per_thread) {
                    keys.push_back(part_key);
                    part_key.clear();
                    thread_key_count = 0;
                }

                found = true;
                break;
            }
        }
    } while(found);

    // --------------------

    Threads threads;
    for (size_t i = 0; i < keys.size(); i++)
        threads.push(new WriterThread(keys[i], root));
    threads.go();

    // --------------------

    // And check that everything has been created correctly.

    for (std::vector<std::vector<std::pair<fdb5::Key, std::string> > >::const_iterator it = keys.begin();
         it != keys.end(); ++it) {

        for (std::vector<std::pair<fdb5::Key, std::string> >::const_iterator it2 = it->begin();
             it2 != it->end(); ++it2) {

            pmem::PersistentPtr<PDataNode> pdata = root.getDataNode(it2->first, mgrMock);
            EXPECT(!pdata.null());

            std::string strnode(static_cast<const char*>(pdata->data()), pdata->length());
            EXPECT(strnode == it2->second);
            eckit::Log::error() << "Check success" << std::endl;
        }
    }

}


CASE( "test_fdb5_pmem_pbranchingnode_datasize" )
{
    // Check that the objects haven't changed size unexpectedly
    // (Important as data layout is persistent...)
    // n.b. There is padding on the PBaseNode, which rounds it up in size...
    EXPECT(sizeof(pmem::PersistentMutex) == size_t(64));
    EXPECT(sizeof(PBranchingNode) == size_t(128));
}


CASE( "test_fdb5_pmem_branchingnode_atomicconstructor" )
{
    // Check that the AtomicConstructor correctly picks up the size

    typedef pmem::AtomicConstructor2<PBranchingNode,
                                     PBranchingNode::KeyType,
                                     PBranchingNode::ValueType> BranchingConstructor2;

    EXPECT(BranchingConstructor2("k1", "v1").size() == size_t(128));
    EXPECT(BranchingConstructor2("k1", "v1").type_id() ==
                            pmem::PersistentType<PBranchingNode>::type_id);

    // Check that the cast-to-PBaseNode constructor retains the type_id

    EXPECT(PBranchingNode::BaseConstructor(
                BranchingConstructor2("k1", "v1")).type_id()
                == pmem::PersistentType<PBranchingNode>::type_id);
    EXPECT(PBranchingNode::BaseConstructor(
                BranchingConstructor2("k1", "v1")).type_id()
                != pmem::PersistentType<PBaseNode>::type_id);

}


//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}

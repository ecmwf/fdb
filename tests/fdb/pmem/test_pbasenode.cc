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

#include "pmem/PersistentPtr.h"
#include "test_persistent_helpers.h"

#include <string>

#include "eckit/testing/Test.h"

#include "fdb5/pmem/PBaseNode.h"
#include "fdb5/pmem/PBranchingNode.h"
#include "fdb5/pmem/PDataNode.h"

using namespace eckit::testing;
using namespace fdb5::pmem;
using namespace pmem;

namespace fdb {
namespace test {

//----------------------------------------------------------------------------------------------------------------------

/// Provide a placeholder class to allow testing of the (protected) BaseNode functionality

class ANode : public PBaseNode {

public:  // types

    enum NodeType {
        NULL_NODE = PBaseNode::NULL_NODE,
        DATA_NODE,
        BRANCHING_NODE
    };

public:  // methods

    ANode(ANode::NodeType type, const KeyType& key, const ValueType& value) :
        PBaseNode(PBaseNode::NodeType(int(PBaseNode::NULL_NODE) + type - ANode::NULL_NODE), key, value) {}
};

//----------------------------------------------------------------------------------------------------------------------

/// Define a root type. Each test that does allocation should use a different element in the root object.

const size_t root_elems = 4;

class RootType {

public:  // constructor

    class Constructor : public AtomicConstructor<RootType> {
        virtual void make(RootType& object) const {
            for (size_t i = 0; i < root_elems; i++) {
                object.data_[i].nullify();
            }
        }
    };

public:  // members

    PersistentPtr<PBaseNode> data_[root_elems];
};

//----------------------------------------------------------------------------------------------------------------------

// Only need type_id for RootType. Others defined in Pool.cc

template <>
uint64_t pmem::PersistentType<RootType>::type_id = POBJ_ROOT_TYPE_NUM;

// Create a global fixture, so that this pool is only created once, and destroyed once.

PersistentPtr<RootType> global_root;

struct SuitePoolFixture {

    SuitePoolFixture() : autoPool_(RootType::Constructor()) { global_root = autoPool_.pool_.getRoot<RootType>(); }
    ~SuitePoolFixture() { global_root.nullify(); }

    AutoPool autoPool_;
};

SuitePoolFixture global_fixture;

//----------------------------------------------------------------------------------------------------------------------

CASE("test_fdb5_pmem_pbasenode_construction") {

    class BNInspector : public PBaseNode {
    public:

        BNInspector() : PBaseNode(PBaseNode::NULL_NODE, "key1", "value1") {}

        bool check() const {

            return type_ == PBaseNode::NULL_NODE && idKey_ == std::string("key1") && idValue_ == std::string("value1");
        }
    };

    BNInspector bn;

    EXPECT(bn.check());
}

CASE("test_fdb5_pmem_pbasenode_types") {
    ANode n1(ANode::NULL_NODE, "key11", "value11");

    EXPECT(n1.isNull());
    EXPECT(!n1.isBranchingNode());
    EXPECT(!n1.isDataNode());

    ANode n2(ANode::DATA_NODE, "key11", "value11");

    EXPECT(!n2.isNull());
    EXPECT(!n2.isBranchingNode());
    EXPECT(n2.isDataNode());

    ANode n3(ANode::BRANCHING_NODE, "key11", "value11");

    EXPECT(!n3.isNull());
    EXPECT(n3.isBranchingNode());
    EXPECT(!n3.isDataNode());
}


CASE("test_fdb5_pmem_pbasenode_matches") {
    ANode n(ANode::NULL_NODE, "key11", "value11");

    EXPECT(n.matches("key11", "value11"));
    EXPECT(!n.matches("key11", "value12"));
    EXPECT(!n.matches("key12", "value11"));
    EXPECT(!n.matches("k", "v"));
    EXPECT(!n.matches("key1111", "value1111"));
}


CASE("test_fdb5_pmem_pbasenode_type_validator") {
    // As a base node, it is invalid to directly instantiate a PBaseNode. The type validation should
    // fail for the base type, and pass for PDataNode and PBranchingNode

    EXPECT(!::pmem::PersistentType<PBaseNode>::validate_type_id(::pmem::PersistentType<PBaseNode>::type_id));
    EXPECT(::pmem::PersistentType<PBaseNode>::validate_type_id(::pmem::PersistentType<PDataNode>::type_id));
    EXPECT(::pmem::PersistentType<PBaseNode>::validate_type_id(::pmem::PersistentType<PBranchingNode>::type_id));
}


CASE("test_fdb5_pmem_pbasenode_valid_allocated") {
    // When we allocate PDataNodes and PBranchingNodes into a PBasenode pointer, everything is just fine

    std::string str_data("Testing testing");

    global_root->data_[0].allocate_ctr(
        PDataNode::BaseConstructor(PDataNode::Constructor("AKey", "AValue", str_data.data(), str_data.length())));

    EXPECT(global_root->data_[0].valid());

    global_root->data_[1].allocate_ctr(PBranchingNode::BaseConstructor(
        AtomicConstructor2<PBranchingNode, std::string, std::string>(std::string("key2"), std::string("value2"))));

    EXPECT(global_root->data_[1].valid());

    // However, you cannot allocate a base node. If we abuse the system and essentially force that
    // it happens, things will fail on the valid() call.

    class TweakConstructor : public AtomicConstructor<PBaseNode> {
        virtual void make(PBaseNode& object) const {}
    };

    global_root->data_[2].allocate_ctr(TweakConstructor());

    EXPECT(!global_root->data_[2].valid());
}

//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}

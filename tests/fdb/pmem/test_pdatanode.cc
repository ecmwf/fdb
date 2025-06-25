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

#include <cstddef>
#include <string>

#include "eckit/testing/Test.h"

#include "fdb5/pmem/PDataNode.h"

using namespace eckit::testing;
using namespace fdb5::pmem;

namespace fdb {
namespace test {

//----------------------------------------------------------------------------------------------------------------------

CASE("test_fdb5_pmem_pdatanode_basenode_functionality") {
    std::string buf("Hello there, testing, testing");

    std::vector<char> elem(PDataNode::data_size(buf.length()));

    PDataNode& dn = *new (&elem[0]) PDataNode("key1", "value1", buf.data(), buf.length());

    EXPECT(dn.isDataNode());
    EXPECT(!dn.isNull());
    EXPECT(!dn.isBranchingNode());

    EXPECT(dn.matches("key1", "value1"));
    EXPECT(!dn.matches("key1", "value2"));
    EXPECT(!dn.matches("key2", "value1"));
}


CASE("test_fdb5_pmem_pdatanode_data") {
    std::string buf("Hello there, testing, testing");

    std::vector<char> elem(PDataNode::data_size(buf.length()));

    PDataNode& dn = *new (&elem[0]) PDataNode("key1", "value1", buf.data(), buf.length());

    EXPECT(dn.length() == eckit::Length(buf.length()));
    EXPECT(dn.data() == (const void*)(&elem[40]));

    std::string check_str(static_cast<const char*>(dn.data()), dn.length());
    EXPECT(buf == check_str);
}


CASE("test_fdb5_pmem_pdatanode_datasize") {
    // Check that the objects haven't changed size unexpectedly
    // (Important as data layout is persistent...)
    // n.b. There is padding on the PBaseNode, which rounds it up in size...
    EXPECT(sizeof(PDataNode) == size_t(48));

    EXPECT(PDataNode::data_size(4) == size_t(44));
    EXPECT(PDataNode::data_size(8) == size_t(48));
    EXPECT(PDataNode::data_size(4096) == size_t(4136));
}


CASE("test_fdb5_pmem_datanode_atomicconstructor") {
    // Check that the AtomicConstructor correctly picks up the size

    EXPECT(PDataNode::Constructor("k1", "v1", NULL, 15).size() == size_t(55));
    EXPECT(PDataNode::Constructor("k1", "v1", NULL, 15).type_id() == pmem::PersistentType<PDataNode>::type_id);

    // Check that the cast-to-PBaseNode constructor retains the type_id

    EXPECT(PDataNode::BaseConstructor(PDataNode::Constructor("k1", "v1", NULL, 15)).type_id() ==
           pmem::PersistentType<PDataNode>::type_id);
    EXPECT(PDataNode::BaseConstructor(PDataNode::Constructor("k1", "v1", NULL, 15)).type_id() !=
           pmem::PersistentType<PBaseNode>::type_id);
}

//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace fdb

int main(int argc, char** argv) {
    return run_tests(argc, argv);
}

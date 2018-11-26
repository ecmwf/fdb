/*
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-3.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Feb 2016


#include "eckit/log/Log.h"
#include "eckit/log/TimeStamp.h"

#include "pmem/PersistentPtr.h"

#include "fdb5/pmem/DataPool.h"
#include "fdb5/pmem/PDataRoot.h"
#include "fdb5/pmem/PRoot.h"
#include "fdb5/LibFdb.h"

#include <unistd.h>


using namespace eckit;
using namespace pmem;


namespace fdb5 {
namespace pmem {


// ---------------------------------------------------------------------------------------------------------------------

std::string data_pool_name(size_t index) {
    std::stringstream ss;

    ss << "data-" << index;
    return ss.str();
}

std::string data_pool_path(const PathName& poolDir, size_t index) {
    return poolDir / data_pool_name(index);
}


DataPool::DataPool(const PathName& poolDir, size_t index) :
    PersistentPool(data_pool_path(poolDir, index), data_pool_name(index)) {

    ASSERT(root().valid());

    Log::debug<LibFdb>() << "Opened persistent pool created at: " << TimeStamp(root().created()) << std::endl;
}


DataPool::DataPool(const PathName& poolDir, size_t index, const size_t size) :
    PersistentPool(data_pool_path(poolDir, index), size, data_pool_name(index), PRoot::Constructor(PRoot::DataClass)) {}


DataPool::~DataPool() {}


void DataPool::buildRoot() {
    // n.b. cannot use baseRoot yet, as not yet valid...
    PersistentPtr<PRoot> rt = getRoot<PRoot>();
    ASSERT(rt.valid());

    // n.b. null arguments. Arguments only apply for index root.
    rt->buildRoot(Key(), "");
}


bool DataPool::finalised() const {
    return root().finalised();
}

void DataPool::finalise() {
    root().finalise();
}


PersistentPtr<PRoot> DataPool::baseRoot() const {
    PersistentPtr<PRoot> rt = getRoot<PRoot>();

    ASSERT(rt.valid());
    ASSERT(rt->valid());
    ASSERT(rt->root_class() == PRoot::DataClass);

    return rt;
}


PDataRoot& DataPool::root() const {

    PDataRoot& rt = baseRoot()->dataRoot();

    ASSERT(rt.valid());
    return rt;
}

// ---------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace tree

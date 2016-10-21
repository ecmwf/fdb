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
#include "eckit/types/FixedString.h"

#include "pmem/PersistentPtr.h"

#include "fdb5/pmem/DataPool.h"

#include <unistd.h>


using namespace eckit;
using namespace pmem;


namespace fdb5 {
namespace pmem {

// ---------------------------------------------------------------------------------------------------------------------


class PDataRoot {

public: // Construction objects

    class Constructor : public AtomicConstructor<PDataRoot> {
    public: // methods
        Constructor() {}
        virtual void make(PDataRoot& object) const;
    };

public: // methods

    bool valid() const;

    const time_t& created() const;

    bool finalised() const;

    void finalise();

private: // members

    eckit::FixedString<8> tag_;

    unsigned short int version_;

    time_t created_;

    long createdBy_;

    bool finalised_;
};


// A consistent definition of the tag for comparison purposes.
const eckit::FixedString<8> PDataRootTag = "66FDB566";
const unsigned short int PDataRootVersion = 1;

// n.b. This also has POBJ_ROOT_TYPE_NUM, as it is a root element (of a different type)
template<> uint64_t PersistentPtr<fdb5::pmem::PDataRoot>::type_id = POBJ_ROOT_TYPE_NUM;


void PDataRoot::Constructor::make(PDataRoot& object) const {
    object.tag_ = PDataRootTag;
    object.version_ = PDataRootVersion;
    object.created_ = time(0);
    object.createdBy_ = getuid();
    object.finalised_ = false;
}

bool PDataRoot::valid() const {

    if (tag_ != PDataRootTag) {
        Log::error() << "Persistent root tag does not match" << std::endl;
        return false;
    }

    if (version_ != PDataRootVersion) {
        Log::error() << "Invalid persistent root version" << std::endl;
        return false;
    }

    return true;
}

const time_t& PDataRoot::created() const {
    return created_;
}

bool PDataRoot::finalised() const {
    return finalised_;
}

void PDataRoot::finalise() {

    finalised_ = true;
    ::pmemobj_persist(::pmemobj_pool_by_ptr(&finalised_), &finalised_, sizeof(finalised_));
}

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

    ASSERT(getRoot<PDataRoot>()->valid());

    Log::info() << "Opened persistent pool created at: " << TimeStamp(getRoot<PDataRoot>()->created()) << std::endl;
}


DataPool::DataPool(const PathName& poolDir, size_t index, const size_t size) :
    PersistentPool(data_pool_path(poolDir, index), size, data_pool_name(index), PDataRoot::Constructor()) {}


DataPool::~DataPool() {}

bool DataPool::finalised() const {
    return getRoot<PDataRoot>()->finalised();
}

void DataPool::finalise() {
    getRoot<PDataRoot>()->finalise();
}

// ---------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace tree

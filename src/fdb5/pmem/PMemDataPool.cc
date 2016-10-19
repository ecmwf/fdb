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

#include "fdb5/pmem/PMemDataPool.h"

#include <unistd.h>


using namespace eckit;
using namespace pmem;


namespace fdb5 {

// ---------------------------------------------------------------------------------------------------------------------


class PMemDataRoot {

public: // Construction objects

    class Constructor : public pmem::AtomicConstructor<PMemDataRoot> {
    public: // methods
        Constructor() {}
        virtual void make(PMemDataRoot& object) const;
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
const eckit::FixedString<8> PMemDataRootTag = "66FDB566";
const unsigned short int PMemDataRootVersion = 1;

// n.b. This also has POBJ_ROOT_TYPE_NUM, as it is a root element (of a different type)
template<> uint64_t pmem::PersistentPtr<fdb5::PMemDataRoot>::type_id = POBJ_ROOT_TYPE_NUM;


void PMemDataRoot::Constructor::make(PMemDataRoot& object) const {
    object.tag_ = PMemDataRootTag;
    object.version_ = PMemDataRootVersion;
    object.created_ = time(0);
    object.createdBy_ = getuid();
    object.finalised_ = false;
}

bool PMemDataRoot::valid() const {

    if (tag_ != PMemDataRootTag) {
        Log::error() << "Persistent root tag does not match" << std::endl;
        return false;
    }

    if (version_ != PMemDataRootVersion) {
        Log::error() << "Invalid persistent root version" << std::endl;
        return false;
    }

    return true;
}

const time_t& PMemDataRoot::created() const {
    return created_;
}

bool PMemDataRoot::finalised() const {
    return finalised_;
}

void PMemDataRoot::finalise() {

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


PMemDataPool::PMemDataPool(const PathName& poolDir, size_t index) :
    PersistentPool(data_pool_path(poolDir, index), data_pool_name(index)) {

    ASSERT(getRoot<PMemDataRoot>()->valid());

    Log::info() << "Opened persistent pool created at: " << TimeStamp(getRoot<PMemDataRoot>()->created()) << std::endl;
}


PMemDataPool::PMemDataPool(const PathName& poolDir, size_t index, const size_t size) :
    PersistentPool(data_pool_path(poolDir, index), size, data_pool_name(index), PMemDataRoot::Constructor()) {}


PMemDataPool::~PMemDataPool() {}

bool PMemDataPool::finalised() const {
    return getRoot<PMemDataRoot>()->finalised();
}

void PMemDataPool::finalise() {
    getRoot<PMemDataRoot>()->finalise();
}

// ---------------------------------------------------------------------------------------------------------------------

} // namespace tree

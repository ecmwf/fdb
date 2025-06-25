/*
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-3.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date   Sept 2016

#include "fdb5/pmem/PDataRoot.h"

#include <unistd.h>
#include "pmem/PersistentPtr.h"

using namespace eckit;

namespace fdb5 {
namespace pmem {

// -------------------------------------------------------------------------------------------------

PDataRoot::PDataRoot() :
    tag_(PDataRootTag), version_(PDataRootVersion), created_(time(0)), createdBy_(getuid()), finalised_(false) {}


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

void PDataRoot::print(std::ostream& s) const {
    s << "PDataRoot(0x" << this << ")";
}

// -------------------------------------------------------------------------------------------------

}  // namespace pmem
}  // namespace fdb5

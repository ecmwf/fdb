/*
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-3.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Sept 2016

#include "eckit/log/Log.h"

#include "fdb5/pmem/PRoot.h"
#include "fdb5/pmem/PIndexRoot.h"
#include "fdb5/pmem/PDataRoot.h"

#include <unistd.h>

using namespace eckit;
using namespace pmem;


namespace fdb5 {
namespace pmem {

// -------------------------------------------------------------------------------------------------


PRoot::PRoot(RootClass cls) :
    tag_(PRootTag),
    version_(PRootVersion),
    rootSize_(sizeof(PRoot)),
    created_(time(0)),
    createdBy_(getuid()),
    class_(cls),
    indexRoot_(),
    dataRoot_() {

    if (class_ == IndexClass) {
        indexRoot_.allocate();
    } else {
        ASSERT(class_ == DataClass);
        dataRoot_.allocate();
    }
}

/*
 * We can use whatever knowledge we have to test the validity of the structure.
 *
 * For now, we just check that the tag is set (i.e. it is initialised).
 */
bool PRoot::valid() const {

    if (tag_ != PRootTag) {
        Log::error() << "Persistent root tag does not match" << std::endl;
        return false;
    }

    if (rootSize_ != sizeof(PRoot)) {
        Log::error() << "Invalid (old) structure size for persistent root" << std::endl;
        return false;
    }

    if (version_ != PRootVersion) {
        Log::error() << "Invalid persistent root version" << std::endl;
        return false;
    }

    if ((class_ == IndexClass && ( indexRoot_.null() || !dataRoot_.null())) ||
        (class_ == DataClass  && (!indexRoot_.null() ||  dataRoot_.null())) ||
        (class_ != IndexClass && class_ != DataClass)) {

        Log::error() << "Inconsistent root node" << std::endl;
        return false;
    }

    return true;
}

const time_t& PRoot::created() const {
    return created_;
}


void PRoot::print(std::ostream& s) const {
    s << "PRoot(0x" << this << ")";
}


PRoot::RootClass PRoot::root_class() const {
    return class_;
}


PIndexRoot& PRoot::indexRoot() const {
    return *indexRoot_;
}


PDataRoot& PRoot::dataRoot() const {
    return *dataRoot_;
}

// -------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

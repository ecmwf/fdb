/*
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-3.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Feb 2016

#include "eckit/io/DataBlob.h"
#include "eckit/log/Log.h"
#include "eckit/parser/JSONDataBlob.h"
#include "eckit/types/Types.h"

#include "fdb5/pmem/PMemRoot.h"

using namespace eckit;
using namespace pmem;


namespace fdb5 {

// -------------------------------------------------------------------------------------------------


PMemRoot::Constructor::Constructor() {}


void PMemRoot::Constructor::make(PMemRoot& object) const {

    object.tag_ = PMemRootTag;
    object.version_ = PMemRootVersion;
}

// -------------------------------------------------------------------------------------------------

/*
 * We can use whatever knowledge we have to test the validity of the structure.
 *
 * For now, we just check that the tag is set (i.e. it is initialised).
 */
bool PMemRoot::valid() const {

    return (tag_ == PMemRootTag && version_ == PMemRootVersion);
}


// -------------------------------------------------------------------------------------------------

} // namespace tree

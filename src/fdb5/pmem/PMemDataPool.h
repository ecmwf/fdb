/*
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-3.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Feb 2016


#ifndef fdb5_pmem_PMemDataPool_H
#define fdb5_pmem_PMemDataPool_H

#include "pmem/PersistentPool.h"


// -------------------------------------------------------------------------------------------------

// Forward declaration
namespace pmem {
    template <typename T> class PersistentPtr;
}

// -------------------------------------------------------------------------------------------------

namespace fdb5 {

// -------------------------------------------------------------------------------------------------

/*
 * Here we define the persistent pool used in this project
 *
 * --> The TreePool needs to know:
 *
 *       i) The data type of the root
 *      ii) How to construct the root object if required
 *     iii) How to map type_ids to actual types
 */

class PMemDataPool : public pmem::PersistentPool {

public: // methods

    PMemDataPool(const eckit::PathName& poolDir, size_t poolIndex);

    PMemDataPool(const eckit::PathName& poolDir, size_t poolIndex, size_t size);

    ~PMemDataPool();

    bool finalised() const;
    void finalise();
};

// -------------------------------------------------------------------------------------------------

} // namespace fdb5


#endif // fdb5_pmem_PMemDataPool_H

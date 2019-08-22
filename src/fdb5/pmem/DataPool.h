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
/// @date   Feb 2016


#ifndef fdb5_pmem_DataPool_H
#define fdb5_pmem_DataPool_H

#include "pmem/PersistentPool.h"


// -------------------------------------------------------------------------------------------------

// Forward declaration
namespace pmem {
    template <typename T> class PersistentPtr;
}

// -------------------------------------------------------------------------------------------------

namespace fdb5 {
namespace pmem {

class PRoot;
class PDataRoot;

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

class DataPool : public ::pmem::PersistentPool {

public: // methods

    DataPool(const eckit::PathName& poolDir, size_t poolIndex);

    DataPool(const eckit::PathName& poolDir, size_t poolIndex, size_t size);

    ~DataPool();

    /// Normally we allocate persistent objects by passing in their subcomponents. We cannot do
    /// that with a root object, as it is allocated at pool creation time.
    ///
    /// --> buildRoot() should be called immediately after pool creation to initialise the root.
    void buildRoot();

    bool finalised() const;
    void finalise();

private: // methods

    ::pmem::PersistentPtr<PRoot> baseRoot() const;
    PDataRoot& root() const;
};


// -------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5


#endif // fdb5_pmem_DataPool_H

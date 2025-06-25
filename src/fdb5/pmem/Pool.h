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

#ifndef fdb5_pmem_Pool_H
#define fdb5_pmem_Pool_H

#include "pmem/PersistentPool.h"

#include "fdb5/database/Key.h"


// -------------------------------------------------------------------------------------------------

// Forward declaration
namespace pmem {
template <typename T>
class PersistentPtr;
}

// -------------------------------------------------------------------------------------------------

namespace fdb5 {

namespace pmem {

class PRoot;
class PIndexRoot;

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

class Pool : public ::pmem::PersistentPool {

public:  // methods

    Pool(const eckit::PathName& path, const std::string& name);

    Pool(const eckit::PathName& path, const size_t size, const std::string& name,
         const ::pmem::AtomicConstructor<PRoot>& constructor);

    ~Pool();

    /// Normally we allocate persistent objects by passing in their subcomponents. We cannot do
    /// that with a root object, as it is allocated at pool creation time.
    ///
    /// --> buildRoot() should be called immediately after pool creation to initialise the root.
    void buildRoot(const Key& dbKey, const eckit::PathName& schemaPath);

    static bool exists(const eckit::PathName& poolDir);

    static Pool* obtain(const eckit::PathName& poolDir, const size_t size, const Key& dbKey,
                        const eckit::PathName& schemaPath);

    ::pmem::PersistentPtr<PRoot> baseRoot() const;
    PIndexRoot& root() const;

private:  // methods

    static eckit::PathName poolMaster(const eckit::PathName& poolDir);
};

// -------------------------------------------------------------------------------------------------

}  // namespace pmem
}  // namespace fdb5


#endif  // fdb5_pmem_Pool_H

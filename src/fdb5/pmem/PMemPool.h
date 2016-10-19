/*
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-3.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Feb 2016


#ifndef fdb5_pmem_PMemPool_H
#define fdb5_pmem_PMemPool_H

#include "pmem/PersistentPool.h"


// -------------------------------------------------------------------------------------------------

// Forward declaration
namespace pmem {
    template <typename T> class PersistentPtr;
}

// -------------------------------------------------------------------------------------------------

namespace fdb5 {

class PMemRoot;

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

class PMemPool : public pmem::PersistentPool {

public: // methods

    PMemPool(const eckit::PathName& path, const std::string& name);

    PMemPool(const eckit::PathName& path, const size_t size, const std::string& name,
             const pmem::AtomicConstructor<PMemRoot>& constructor);

    ~PMemPool();

    static PMemPool* obtain(const eckit::PathName& poolDir, const size_t size);

    pmem::PersistentPtr<PMemRoot> root() const;

};

// -------------------------------------------------------------------------------------------------

} // namespace fdb5


#endif // fdb5_pmem_PMemPool_H

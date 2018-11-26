/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Sep 2016


#ifndef fdb5_pmem_PRoot_H
#define fdb5_pmem_PRoot_H

#include "eckit/memory/NonCopyable.h"
#include "eckit/types/FixedString.h"

#include "pmem/PersistentPtr.h"
#include "pmem/PersistentMutex.h"

#include "fdb5/database/Key.h"

#include <ctime>


namespace fdb5 {

class Key;

namespace pmem {

class PIndexRoot;
class PDataRoot;

// -------------------------------------------------------------------------------------------------

/// The primary root object
/// @note We have a "primary" root object, that points to either the real index or data data
///       roots depending on the pool we are in. This permits having unique mapping between
///       the type_ids (and correct versioning) for all objects, including the two differing
///       root objects (as the root object MUST have the type_id POBJ_ROOT_TYPE_NUM).

class PRoot : public eckit::NonCopyable {

public: // types

    enum RootClass {
        IndexClass = 0,
        DataClass
    };

    typedef ::pmem::AtomicConstructor1<PRoot, RootClass> Constructor;

public: // methods

    PRoot(RootClass cls);

    /// Normally we allocate persistent objects by passing in their subcomponents. We cannot do
    /// that with a root object, as it is allocated at pool creation time.
    ///
    /// --> buildRoot() should be called immediately after pool creation to initialise the root.
    void buildRoot(const Key& dbKey, const eckit::PathName& schemaPath);

    bool valid() const;

    const time_t& created() const;

    void print(std::ostream& s) const;

    RootClass root_class() const;

    PIndexRoot& indexRoot() const;
    PDataRoot& dataRoot() const;

private: // members

    eckit::FixedString<8> tag_;

    unsigned short int version_;

    /// Store the size of the PMemRoot object. Allows some form of sanity check
    /// when it is being opened, that everything is safe.
    unsigned short int rootSize_;

    time_t created_;

    long createdBy_;

    RootClass class_;

    /// Access to the "real" root objects.

    ::pmem::PersistentPtr<PIndexRoot> indexRoot_;
    ::pmem::PersistentPtr<PDataRoot> dataRoot_;

    /// Ensure that only one thread at a time tries to allocate a root!
    ::pmem::PersistentMutex rootMutex_;

private: // friends

    friend std::ostream& operator<<(std::ostream& s, const PRoot& r) { r.print(s); return s; }
};


// A consistent definition of the tag for comparison purposes.

const eckit::FixedString<8> PRootTag = "99FDB599";
const unsigned short int PRootVersion = 2;


// -------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif // fdb5_pmem_PRoot_H

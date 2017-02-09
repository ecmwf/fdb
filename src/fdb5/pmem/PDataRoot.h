/*
 * (C) Copyright 1996-2015 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */


/// @author Simon Smart
/// @date   Sep 2016


#ifndef fdb5_pmem_PDataRoot_H
#define fdb5_pmem_PDataRoot_H

#include "eckit/memory/NonCopyable.h"
#include "eckit/types/FixedString.h"

namespace fdb5 {
namespace pmem {

// -------------------------------------------------------------------------------------------------

/// A root object for a persistent data pool
/// @note Unlike all other root objects (in the examples) this does NOT contain PersistentPtrs to
///       the data - and as such the data is inaccessible directly within the root. The data is
///       pointed to with PersistentPtrs from another pool (linked from a PIndexRoot).

class PDataRoot : public eckit::NonCopyable {

public: // methods

    PDataRoot();

    bool valid() const;

    const time_t& created() const;

    bool finalised() const;

    void finalise();

    void print(std::ostream& s) const;

private: // members

    eckit::FixedString<8> tag_;

    unsigned short int version_;

    time_t created_;

    long createdBy_;

    bool finalised_;

private: // friends

    friend std::ostream& operator<<(std::ostream& s, const PDataRoot& r) { r.print(s); return s; }
};


// A consistent definition of the tag for comparison purposes.
const eckit::FixedString<8> PDataRootTag = "66FDB566";
const unsigned short int PDataRootVersion = 2;


// -------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif // fdb5_pmem_PDataRoot_H

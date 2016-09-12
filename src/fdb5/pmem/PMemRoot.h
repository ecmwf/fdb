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


#ifndef fdb5_pmem_PMemRoot_H
#define fdb5_pmem_PMemRoot_H

#include "eckit/memory/NonCopyable.h"
#include "eckit/types/FixedString.h"
#include "eckit/types/Types.h"

#include "pmem/AtomicConstructor.h"

#include <ctime>


namespace fdb5 {

// -------------------------------------------------------------------------------------------------

// N.B. This is to be stored in PersistentPtr --> NO virtual behaviour.

class PMemRoot {

public: // Construction objects

    class Constructor : public pmem::AtomicConstructor<PMemRoot> {
    public: // methods
        Constructor();
        virtual void make(PMemRoot& object) const;
    };


public: // methods

    bool valid() const;

    const time_t& created() const;

private: // members

    eckit::FixedString<8> tag_;

    unsigned short int version_;

    /// Store the size of the PMemRoot object. Allows some form of sanity check
    /// when it is being opened, that everything is safe.
    unsigned short int rootSize_;

    time_t created_;

    long createdBy_;

private: // friends

    friend class TreeObject;
};


// A consistent definition of the tag for comparison purposes.
const eckit::FixedString<8> PMemRootTag = "99FDB599";
const unsigned short int PMemRootVersion = 1;


// -------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_pmem_PMemRoot_H

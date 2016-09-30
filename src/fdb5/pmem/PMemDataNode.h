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


#ifndef fdb5_pmem_PMemDataNode_H
#define fdb5_pmem_PMemDataNode_H

#include "eckit/types/Types.h"

#include "pmem/AtomicConstructor.h"
#include "pmem/PersistentVector.h"

#include "fdb5/pmem/PMemBaseNode.h"


namespace fdb5 {

class Key;

// -------------------------------------------------------------------------------------------------

// N.B. This is to be stored in PersistentPtr --> NO virtual behaviour.

class PMemDataNode : public PMemBaseNode {

public: // Construction objects

    class Constructor : public pmem::AtomicConstructor<PMemDataNode>, public PMemBaseNode::Constructor {
    public: // methods
        Constructor(const KeyType& key, const ValueType& value, const void* data, eckit::Length length);
        virtual size_t size() const;
        virtual void make(PMemDataNode& object) const;
    private:
        eckit::Length length_;
        const void* data_;
    };

    /// A wrapper to allow us to pass a PMemDataNode constructor into allocate for the base class
    /// TODO: Can we create a more generic templated AtomicConstructor-caster.
    class BaseConstructor : public pmem::AtomicConstructor<PMemBaseNode> {
    public: // methods
        BaseConstructor(const PMemDataNode::Constructor& ctr);
        virtual void make(PMemBaseNode& object) const;
        virtual size_t size() const;

    private: // members
        const PMemDataNode::Constructor& ctr_;
    };

private: // members

    eckit::Length length_;

    // We use a length of 8 to ensure (a) this is legal c++, and (b) there is no possibility of the complier
    // adding unecessary padding due to an unreasonable desire for structure alignment.
    char data_[8];

};

// -------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_pmem_PMemDataNode_H

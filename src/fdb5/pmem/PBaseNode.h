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


#ifndef fdb5_pmem_PBaseNode_H
#define fdb5_pmem_PBaseNode_H

#include "eckit/types/FixedString.h"

#include <iosfwd>


namespace fdb5 {
namespace pmem {

// -------------------------------------------------------------------------------------------------

class PBranchingNode;
class PDataNode;

// N.B. This is to be stored in PersistentPtr --> NO virtual behaviour.

class PBaseNode {

public: // Types

    typedef eckit::FixedString<12> KeyType;
    typedef eckit::FixedString<12> ValueType;

protected: // Types

    enum NodeType {
        NULL_NODE,
        DATA_NODE,
        BRANCHING_NODE
    };

    /// Base class constructor. Note that this does NOT derive from AtomicConstructor. It needs
    /// to be mixed in with an AtomicConstructor<T> to form a meaningful derived class constructor.

    class Constructor {
    public: // methods
        Constructor(NodeType type, const KeyType& key, const ValueType& value);
        void constructBase(PBaseNode& object) const;
    private: // members
        NodeType type_;
        KeyType key_;
        ValueType value_;
    };

private: // Construction objects

    // This object MUST NOT be constructed manually. Only as part of a derived base
    PBaseNode() {}

public: // methods

    bool isNull() const;
    bool isBranchingNode() const;
    bool isDataNode() const;

    PBranchingNode& asBranchingNode();
    PDataNode& asDataNode();

    bool matches(const KeyType& key, const ValueType& value) const;

protected: // members

    unsigned int type_;

    /// The key:value pair that identifies which node we have reached.
    /// @note Fixed with of 12 characters. There is no reason that this could not be updated.
    KeyType idKey_;
    ValueType idValue_;

private: // friends

    friend std::ostream& operator<< (std::ostream&, const PBaseNode&);
};


// -------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif // fdb5_pmem_PBaseNode_H

/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date Sep 2012

#ifndef fdb5_PMemIndex_H
#define fdb5_PMemIndex_H

#include "eckit/eckit.h"

#include "fdb5/database/Index.h"

namespace fdb5 {

class PMemBranchingNode;

//----------------------------------------------------------------------------------------------------------------------


//class PMemIndex : public Index {
//
//public: // types
//
//
//public: // methods
//
//    PMemIndex(const Key &key, PMemBranchingNode& node);
//
//    virtual ~PMemIndex();
//
//private: // methods
//
//    virtual void open();
//    virtual void close();
//    virtual void reopen();
//
//    virtual bool get( const Key &key, Field &field ) const;
//    virtual void add( const Key &key, const Field &field );
//    virtual void flush();
//    virtual void entries(EntryVisitor &visitor) const;
//
//    virtual void print( std::ostream &out ) const;
//    virtual void dump(std::ostream& out, const char* indent, bool simple = false) const;
//
//private: // members
//
//    PMemBranchingNode& node_;
//
//};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

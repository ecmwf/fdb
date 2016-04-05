/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Op.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb_Op_H
#define fdb_Op_H

#include <iosfwd>

#include "eckit/memory/NonCopyable.h"

class MarsTask;

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class Op : public eckit::NonCopyable {

public: // methods

	Op();
    
    virtual ~Op();

    virtual void enter() = 0;
    virtual void leave() = 0;

    virtual void execute(const MarsTask& task, Key& key, Op& tail) = 0;

    virtual void fail(const MarsTask& task, Key& key, Op& tail) = 0;

    virtual void print( std::ostream& out ) const = 0;

    friend std::ostream& operator<<(std::ostream& s,const Op& x);

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

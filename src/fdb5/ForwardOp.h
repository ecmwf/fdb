/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   ForwardOp.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb_ForwardOp_H
#define fdb_ForwardOp_H

#include "fdb5/Op.h"

namespace fdb {

//----------------------------------------------------------------------------------------------------------------------

class ForwardOp : public fdb::Op {

public: // methods

    ForwardOp(Op& parent);

    /// Destructor
    
    virtual ~ForwardOp();

private: // methods

    virtual void descend();

    virtual void execute(const FdbTask& task, Key& key);

    virtual void fail(const FdbTask& task, Key& key);

    virtual void print( std::ostream& out ) const;

private:

    Op& parent_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

#endif

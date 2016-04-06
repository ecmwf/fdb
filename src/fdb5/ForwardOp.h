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

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class ForwardOp : public fdb5::Op {

public: // methods

    ForwardOp(Op& parent);

    /// Destructor
    
    virtual ~ForwardOp();

private: // methods

    virtual void enter(const std::string& param, const std::string& value);
    virtual void leave();

    virtual void execute(const MarsTask& task, Key& key, Op &tail);

    virtual void fail(const MarsTask& task, Key& key, Op &tail);

    virtual void print( std::ostream& out ) const;

private:

    Op& parent_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

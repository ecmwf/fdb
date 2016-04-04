/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   RetrieveOp.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb_RetrieveOp_H
#define fdb_RetrieveOp_H

#include "eckit/io/MultiHandle.h"

#include "fdb5/Op.h"

namespace fdb5 {

class DB;

//----------------------------------------------------------------------------------------------------------------------

class RetrieveOp : public fdb5::Op {

public: // methods

    RetrieveOp(const DB& db, eckit::MultiHandle& result);

    /// Destructor
    
    virtual ~RetrieveOp();

private: // methods

    virtual void descend();

    virtual void execute(const MarsTask& task, Key& key);

    virtual void fail(const MarsTask& task, Key& key);

private:

    virtual void print( std::ostream& out ) const;

private: // members

    const DB& db_;

    eckit::MultiHandle& result_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

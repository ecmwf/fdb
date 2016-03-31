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

#include "eckit/memory/NonCopyable.h"

namespace marskit { class MarsRequest; }

namespace fdb {

class FdbTask;

//----------------------------------------------------------------------------------------------------------------------

class Op : public eckit::NonCopyable {

public: // methods

	Op();
    
    virtual ~Op();

    virtual void descend() = 0;

    virtual void execute(const FdbTask& task, marskit::MarsRequest& field) = 0;

    virtual void fail(const FdbTask& task, marskit::MarsRequest& field) = 0;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb

#endif

/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   NotFound.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb_NotFound_H
#define fdb_NotFound_H

#include "eckit/exception/Exceptions.h"

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class NotFound : public eckit::Exception {

public: // methods

    NotFound(const Key& r);
    
    ~NotFound() throw();

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

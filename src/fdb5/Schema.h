/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Schema.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb_Schema_H
#define fdb_Schema_H

#include <vector>
#include <string>

#include "eckit/memory/NonCopyable.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class Schema : public std::vector<std::string>,
               private eckit::NonCopyable {

public: // methods

    Schema();
    
    virtual ~Schema();

    friend std::ostream& operator<<(std::ostream& s, const Schema& x);

protected: // methods

    virtual void print( std::ostream& out ) const = 0;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

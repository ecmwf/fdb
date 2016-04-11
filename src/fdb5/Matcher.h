/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Matcher.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_Matcher_H
#define fdb5_Matcher_H

#include <iosfwd>

#include "eckit/memory/NonCopyable.h"

class MarsTask;

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class Matcher : public eckit::NonCopyable {

public: // methods

	Matcher();
    
    virtual ~Matcher();

    virtual bool eval(const std::string& keyword, const Key& key) const = 0;

    friend std::ostream& operator<<(std::ostream& s,const Matcher& x);

private: // methods

    virtual void print( std::ostream& out ) const = 0;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

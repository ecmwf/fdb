/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Rule.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_Rule_H
#define fdb5_Rule_H

#include <iosfwd>
#include <vector>

#include "eckit/memory/NonCopyable.h"

namespace fdb5 {

class Predicate;

//----------------------------------------------------------------------------------------------------------------------

class Rule : public eckit::NonCopyable {

public: // methods

    /// Takes ownership of vectors
    Rule(std::vector<Predicate*>& predicates, std::vector<Rule*>& rules);
    
    ~Rule();

    void dump(std::ostream& s, size_t depth = 0) const;

private: // methods

    friend std::ostream& operator<<(std::ostream& s,const Rule& x);

    void print( std::ostream& out ) const;

private: // members

    std::vector<Predicate*> predicates_;
    std::vector<Rule*>      rules_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Key.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb_Key_H
#define fdb_Key_H

#include <map>
#include <string>
#include <vector>

#include "eckit/exception/Exceptions.h"
#include "eckit/types/Types.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class Key {

public: // methods

    Key();

    void set( const std::string& k, const std::string& v ) { keys_[k] = v; }

    const std::string& get( const std::string& k ) const {
        eckit::StringDict::const_iterator i = keys_.find(k);
        ASSERT( i != keys_.end() );
        return i->second;
    }

    const eckit::StringDict& dict() const { return keys_; }

    void clear();

    Key subkey(const std::vector<std::string>& pattern) const;

    bool match(const Key& partial) const;

    friend std::ostream& operator<<(std::ostream& s,const Key& x) {
        x.print(s);
        return s;
    }

private: // members

    void print( std::ostream& out ) const;

    eckit::StringDict keys_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

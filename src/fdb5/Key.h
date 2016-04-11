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

#ifndef fdb5_Key_H
#define fdb5_Key_H

#include <map>
#include <string>
#include <vector>

#include "eckit/types/Types.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class Key {

public: // methods

    Key();

    Key(const eckit::StringDict& keys);

    void set(const std::string& k, const std::string& v);
    void unset(const std::string& k) { keys_.erase(k); }

    const std::string& get( const std::string& k ) const;

    const eckit::StringDict& dict() const { return keys_; }

    void clear();

    Key subkey(const std::vector<std::string>& pattern) const;

    bool match(const Key& partial) const;

    bool operator< (const Key& other) const {
        return keys_ < other.keys_;
    }

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

/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   KeyCollector.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_KeyCollector_H
#define fdb5_KeyCollector_H

#include <iosfwd>
#include <vector>

#include "eckit/memory/NonCopyable.h"
#include "eckit/types/Types.h"

class MarsRequest;

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class KeyCollector : public eckit::NonCopyable {
public: // methods

    virtual ~KeyCollector();

    virtual void collect(const Key& key0,
                         const Key& key1,
                         const Key& key2) = 0;

    virtual void enter(const std::string& keyword, const std::string& value);
    virtual void leave();

    virtual void enter(std::vector<Key>& keys);
    virtual void leave(std::vector<Key>& keys);

    virtual void values(const MarsRequest& request, const std::string& keyword, eckit::StringList& values);

private: // methods

    friend std::ostream& operator<<(std::ostream& s,const KeyCollector& x);

    virtual void print( std::ostream& out ) const = 0;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

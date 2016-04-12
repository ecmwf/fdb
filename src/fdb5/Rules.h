/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Rules.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_Rules_H
#define fdb5_Rules_H

#include <iosfwd>
#include <vector>

#include "eckit/memory/NonCopyable.h"
#include "eckit/filesystem/PathName.h"

class MarsRequest;

namespace fdb5 {

class Key;
class Rule;
class Visitor;

//----------------------------------------------------------------------------------------------------------------------

class Rules : private eckit::NonCopyable {

public: // methods

    Rules();

    ~Rules();


    void expand(const MarsRequest& request, fdb5::Visitor& collector) const;

    void load(const eckit::PathName& path, bool replace = false);

    void dump(std::ostream& s) const;

private: // methods

    void clear();
    void check();

    friend std::ostream& operator<<(std::ostream& s,const Rules& x);

    void print( std::ostream& out ) const;

private: // members

    std::vector<Rule*>      rules_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

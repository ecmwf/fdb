/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Predicate.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_Predicate_H
#define fdb5_Predicate_H

#include <iosfwd>

#include "eckit/memory/NonCopyable.h"
#include "eckit/memory/ScopedPtr.h"

namespace fdb5 {

class Key;
class Matcher;
class TypesRegistry;

//----------------------------------------------------------------------------------------------------------------------

class Predicate : public eckit::NonCopyable {

public: // methods

    Predicate(const std::string &keyword, Matcher *matcher);

    ~Predicate();

    bool match(const Key &key) const;

    void dump( std::ostream &s, const TypesRegistry &registry ) const;

    const std::string &value(const Key &key) const;
    const std::string &defaultValue() const;

    bool optional() const;

    std::string keyword() const;

private: // methods

    friend std::ostream &operator<<(std::ostream &s, const Predicate &x);

    void print( std::ostream &out ) const;

private: // members

    eckit::ScopedPtr<Matcher> matcher_;

    std::string keyword_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

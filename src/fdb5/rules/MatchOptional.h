/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   MatchOptional.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_MatchOptional_H
#define fdb5_MatchOptional_H

#include <iosfwd>
#include <string>

#include "fdb5/rules/Matcher.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class MatchOptional : public Matcher {

public: // methods

    MatchOptional(const std::string &def);

    virtual ~MatchOptional();

    virtual bool match(const std::string &keyword, const Key &key) const;

    virtual void dump(std::ostream &s, const std::string &keyword, const TypesRegistry &registry) const;

private: // methods

    virtual bool optional() const;
    virtual const std::string &value(const Key &, const std::string &keyword) const;
    virtual void print( std::ostream &out ) const;
    virtual const std::string &defaultValue() const;
    virtual void fill(Key &key, const std::string &keyword, const std::string& value) const;


    std::string default_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

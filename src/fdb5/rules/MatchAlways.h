/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   MatchAlways.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_MatchAlways_H
#define fdb5_MatchAlways_H

#include <iosfwd>

#include "fdb5/rules/Matcher.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class MatchAlways : public Matcher {

public: // methods

    MatchAlways();

    virtual ~MatchAlways() override;

    virtual bool match(const std::string &keyword, const Key& key) const override;

    virtual void dump(std::ostream &s, const std::string &keyword, const TypesRegistry &registry) const override;

private: // methods

    virtual void print( std::ostream &out ) const override;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   MatchHidden.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_MatchHidden_H
#define fdb5_MatchHidden_H

#include <iosfwd>
#include <string>

#include "fdb5/rules/Matcher.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class MatchHidden : public Matcher {

public: // methods

    MatchHidden(const std::string &def);

    virtual ~MatchHidden() override;

    virtual bool match(const std::string &keyword, const Key &key) const override;

    virtual void dump(std::ostream &s, const std::string &keyword, const TypesRegistry &registry) const override;

private: // methods

    virtual bool optional() const override;
    virtual const std::string &value(const Key &, const std::string &keyword) const override;
    virtual const std::vector<std::string>& values(const metkit::mars::MarsRequest& rq, const std::string& keyword) const override;
    virtual void print( std::ostream &out ) const override;
    virtual const std::string &defaultValue() const override;


    std::vector<std::string> default_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

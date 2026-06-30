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

#pragma once

#include <iosfwd>
#include <string>

#include "fdb5/rules/Matcher.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class MatchAlways : public Matcher {

public:  // methods

    MatchAlways() = default;

    MatchAlways(eckit::Stream& s);
    bool match(const std::string& /*keyword*/, const Key& /*key*/) const override { return true; }

    bool match(const std::string& /*value*/) const override { return true; }

    void dump(std::ostream& s, const std::string& keyword, const TypesRegistry& registry) const override;

    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }
    static const eckit::ClassSpec& classSpec() { return classSpec_; }

private:  // methods

    void encode(eckit::Stream&) const override;

    void print(std::ostream& out) const override;

private:  // members

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<MatchAlways> reanimator_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

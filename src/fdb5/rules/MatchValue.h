/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   MatchValue.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_MatchValue_H
#define fdb5_MatchValue_H

#include <iosfwd>
#include <string>

#include "fdb5/rules/Matcher.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class MatchValue : public Matcher {

public:  // methods

    MatchValue(std::string value);

    MatchValue(eckit::Stream& stream);

    bool match(const std::string& value) const override;

    bool match(const std::string& keyword, const Key& key) const override;

    void dump(std::ostream& out, const std::string& keyword, const TypesRegistry& registry) const override;

    // streamable
    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }
    static const eckit::ClassSpec& classSpec() { return classSpec_; }

private:  // methods

    void encode(eckit::Stream& out) const override;

    void print(std::ostream& out) const override;

private:  // members

    std::string value_;

    // streamable

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<MatchValue> reanimator_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

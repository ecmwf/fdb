// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   MatchValue.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#pragma once

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

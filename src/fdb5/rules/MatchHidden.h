// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   MatchHidden.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#pragma once

#include <iosfwd>
#include <string>
#include <vector>

#include "fdb5/rules/Matcher.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class MatchHidden : public Matcher {

public:  // methods

    MatchHidden(std::string def);

    MatchHidden(eckit::Stream& s);

    bool match(const std::string& /*value*/) const override { return true; }

    bool match(const std::string& /*keyword*/, const Key& /*key*/) const override { return true; }

    void dump(std::ostream& s, const std::string& keyword, const TypesRegistry& registry) const override;

    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }
    static const eckit::ClassSpec& classSpec() { return classSpec_; }

private:  // methods

    void encode(eckit::Stream& stream) const override;

    bool optional() const override { return true; }

    const std::vector<std::string>& optionalValues() const override;

    const std::string& value(const Key&, const std::string& keyword) const override;
    const std::vector<std::string>& values(const metkit::mars::MarsRequest& rq,
                                           const std::string& keyword) const override;
    const std::string& defaultValue() const override;

    void print(std::ostream& out) const override;

private:  // members

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<MatchHidden> reanimator_;

    std::vector<std::string> default_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5


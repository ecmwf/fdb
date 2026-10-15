// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   MatchOptional.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#pragma once

#include <iosfwd>
#include <optional>
#include <string>
#include <vector>

#include "fdb5/rules/Matcher.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class MatchOptional : public Matcher {

public:  // methods

    MatchOptional(std::string def);

    MatchOptional(eckit::Stream& s);

    bool match(const std::string& value) const override;

    bool match(const std::string& keyword, const Key& key) const override;

    void dump(std::ostream& s, const std::string& keyword, const TypesRegistry& registry) const override;

    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }
    static const eckit::ClassSpec& classSpec() { return classSpec_; }

private:  // methods

    void encode(eckit::Stream&) const override;

    bool optional() const override;
    const std::vector<std::string>& optionalValues() const override;

    const std::string& value(const Key&, const std::string& keyword) const override;
    const std::vector<std::string>& values(const metkit::mars::MarsRequest& rq,
                                           const std::string& keyword) const override;
    void print(std::ostream& out) const override;
    const std::string& defaultValue() const override;
    void fill(Key& key, const std::string& keyword, const std::string& value) const override;

private:  // members

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<MatchOptional> reanimator_;

    std::vector<std::string> default_;
    mutable std::optional<std::vector<std::string>> optionalValues_ = std::nullopt;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5


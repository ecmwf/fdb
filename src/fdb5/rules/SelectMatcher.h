// SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Chris Bradley
/// @date   Sep 2025

#pragma once

#include <optional>
#include <vector>

#include "eckit/config/LocalConfiguration.h"

#include "metkit/mars/Matcher.h"

#include "fdb5/database/Key.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

// A collection of select/exclude matchers, for a single FDB lane.
class SelectMatcher {

    using Matcher = metkit::mars::Matcher;

public:

    SelectMatcher(const eckit::LocalConfiguration& config);

    SelectMatcher(Matcher select, std::vector<Matcher> excludes);

    bool match(const Key& key, Matcher::MatchMissingPolicy matchOnMissing) const;
    bool match(const metkit::mars::MarsRequest& request, Matcher::MatchMissingPolicy matchOnMissing) const;

private:

    template <typename T>  // T is either a MarsRequest or KeyAccessor
    bool matchInner(const T& vals, Matcher::MatchMissingPolicy matchOnMissing) const;

private:

    std::optional<Matcher> select_{std::nullopt};
    std::vector<Matcher> excludes_{};
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
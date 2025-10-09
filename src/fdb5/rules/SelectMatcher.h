/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Chris Bradley
/// @date   Sep 2025

#pragma once

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

    Matcher select_;
    std::vector<Matcher> excludes_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
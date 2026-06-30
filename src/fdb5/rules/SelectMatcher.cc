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

#include "fdb5/rules/SelectMatcher.h"

#include <algorithm>
#include <functional>
#include <optional>
#include <string>

#include "metkit/mars/Parameter.h"

namespace fdb5 {

// ------------------------------------------------------------------------------------------------------

namespace {

class KeyAccessor : public metkit::mars::RequestLike {
public:

    explicit KeyAccessor(const Key& key) : key_(key) {}

    std::optional<values_t> get(const std::string& keyword) const override {
        const auto [it, found] = key_.find(keyword);
        if (!found) {
            return std::nullopt;
        }
        return std::cref(it->second);
    }

private:

    const Key& key_;
};

}  // namespace

// ------------------------------------------------------------------------------------------------------

SelectMatcher::SelectMatcher(const eckit::LocalConfiguration& config) :
    select_{Matcher(config.getString("select", ""), Matcher::Policy::Any)} {

    if (config.has("excludes")) {
        for (const auto& ex : config.getStringVector("excludes")) {
            excludes_.push_back(Matcher(ex, Matcher::Policy::All));
        }
    }
}

SelectMatcher::SelectMatcher(Matcher select, std::vector<Matcher> excludes) : select_(select), excludes_(excludes) {}


bool SelectMatcher::match(const Key& key, Matcher::MatchMissingPolicy matchOnMissing) const {
    return matchInner(KeyAccessor(key), matchOnMissing);
}

bool SelectMatcher::match(const metkit::mars::MarsRequest& request, Matcher::MatchMissingPolicy matchOnMissing) const {
    return matchInner(request, matchOnMissing);
}

template <typename T>  // T is either a MarsRequest or KeyAccessor
bool SelectMatcher::matchInner(const T& vals, Matcher::MatchMissingPolicy matchOnMissing) const {
    if (!select_.match(vals, matchOnMissing)) {
        return false;
    }

    bool excluded = std::any_of(excludes_.begin(), excludes_.end(),
                                [&](const auto& ex) { return ex.match(vals, Matcher::DontMatchOnMissing); });
    return !excluded;
}

// ------------------------------------------------------------------------------------------------------

}  // namespace fdb5

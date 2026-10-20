/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/toc/FileSpaceSelector.h"

#include "fdb5/database/Key.h"

using metkit::mars::Matcher;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
// Helpers

namespace {

/// Adapts fdb5::Key to metkit::mars::RequestLike for use with Matcher.
/// Empty values are treated as absent.
class PartialKeyAdapter : public metkit::mars::RequestLike {
public:

    explicit PartialKeyAdapter(const Key& key) : key_(key) {}

    std::optional<values_t> get(const std::string& keyword) const override {
        const auto [it, found] = key_.find(keyword);
        if (!found || it->second.empty()) {
            return std::nullopt;
        }
        return std::cref(it->second);
    }

private:

    const Key& key_;
};

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

RegexSelector::RegexSelector(eckit::Regex regex) : regex_{std::move(regex)} {}

bool RegexSelector::match(const Key& key) const {
    return regex_.match(key.valuesToString());
}

void RegexSelector::print(std::ostream& out) const {
    out << regex_;
}

//----------------------------------------------------------------------------------------------------------------------

MatchSelector::MatchSelector(metkit::mars::Matcher matcher) : matcher_{std::move(matcher)} {}

bool MatchSelector::match(const Key& key) const {
    return matcher_.match(PartialKeyAdapter(key), Matcher::MatchOnMissing);
}

void MatchSelector::print(std::ostream& out) const {
    out << matcher_;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

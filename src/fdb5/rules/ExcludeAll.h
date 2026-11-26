// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   ExcludeAll.h
/// @author Emanuele Danovaro
/// @date   April 2025

#pragma once

#include <iosfwd>
#include <set>
#include <string>

#include "fdb5/rules/Matcher.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class ExcludeAll : public Matcher {

public:  // methods

    ExcludeAll(const std::set<std::string>& values);
    ExcludeAll(eckit::Stream& s);

    bool match(const std::string& value) const override;

    bool match(const std::string& keyword, const Key& key) const override;

    void dump(std::ostream& s, const std::string& keyword, const TypesRegistry& registry) const override;

    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }
    static const eckit::ClassSpec& classSpec() { return classSpec_; }

private:  // methods

    void encode(eckit::Stream&) const override;

    void print(std::ostream& out) const override;

private:  // members

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<ExcludeAll> reanimator_;

    std::set<std::string> values_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

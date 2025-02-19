/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Key.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#pragma once

#include "fdb5/database/BaseKey.h"
#include "fdb5/types/TypesRegistry.h"

#include <cstddef>
#include <functional>
#include <string>

namespace eckit {
template <class T>
class DenseSet;
}

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

class Rule;

//----------------------------------------------------------------------------------------------------------------------
// KEY

class Key : public BaseKey {
public:  // factory

    static Key parse(const std::string& keyString);

public:  // methods

    using BaseKey::BaseKey;

    std::string type() const override { return "Key"; }

    std::string valuesToString() const;

    /// @throws When "other" doesn't contain all the keys of "this"
    void validateKeys(const Key& other, bool checkAlsoValues = false) const;

    // MATCH

    bool match(const Key& other) const;

    bool match(const metkit::mars::MarsRequest& request) const;

    /// test that, if keys are present in the supplied request, they match the
    /// keys present in the key. Essentially implements a reject-filter
    bool partialMatch(const metkit::mars::MarsRequest& request) const;

    bool matchValues(const std::string& keyword, const eckit::DenseSet<std::string>& values) const;
};

//----------------------------------------------------------------------------------------------------------------------
// TYPED KEY

class TypedKey : public BaseKey {
public:  // methods

    explicit TypedKey(const TypesRegistry& reg) : registry_{reg} {}

    // RULES
    TypedKey(const TypedKey& other)            = delete;
    TypedKey& operator=(const TypedKey& other) = delete;
    TypedKey(TypedKey&& other)                 = delete;
    TypedKey& operator=(TypedKey&& other)      = delete;
    ~TypedKey()                                = default;

    std::string type() const override { return "TypedKey"; }

    Key tidy() const;

    Key canonical() const;

private:  // members

    const TypesRegistry& registry_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

namespace std {

template <>
struct hash<fdb5::Key> {
    size_t operator()(const fdb5::Key& key) const { return std::hash<std::string>()(key.valuesToString()); }
};

}  // namespace std

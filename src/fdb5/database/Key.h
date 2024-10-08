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

#include <cstddef>
#include <functional>
#include <string>

namespace eckit {
template<class T>
class DenseSet;
}

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

class TypesRegistry;
class Rule;

//----------------------------------------------------------------------------------------------------------------------
// KEY

class Key : public BaseKey {
public:  // factory
    static Key parse(const std::string& keyString);

public:  // methods
    using BaseKey::BaseKey;

    explicit Key(const eckit::StringDict& keys) {
        for (const auto& [keyword, value] : keys) { emplace(keyword, value); }
    }

    std::string valuesToString() const override;

    /// @throws When "other" doesn't contain all the keys of "this"
    void validateKeys(const Key& other, bool checkAlsoValues = false) const;

    bool matchValues(const std::string& keyword, const eckit::DenseSet<std::string>& values) const;
};

//----------------------------------------------------------------------------------------------------------------------
// TYPED KEY

class TypedKey : public BaseKey {
public:  // factory
    static TypedKey parse(const std::string& keyString, const TypesRegistry& reg);

public:  // methods
    explicit TypedKey(const TypesRegistry& reg) noexcept : registry_ {reg} { }

    // TypedKey(BaseKey&& key, const TypesRegistry& reg) noexcept : BaseKey {std::move(key)}, registry_ {reg} { }
    // TypedKey(const BaseKey& key, const TypesRegistry& reg) : BaseKey {key}, registry_ {reg} { }

    // RULES

    TypedKey(const TypedKey& other)            = delete;
    TypedKey& operator=(const TypedKey& other) = delete;
    TypedKey(TypedKey&& other)                 = delete;
    TypedKey& operator=(TypedKey&& other)      = delete;
    ~TypedKey()                                = default;

    std::string valuesToString() const override;

    std::string canonicalise(const std::string& keyword, const std::string& value) const;

    /// @note prefer canonicalise to this method
    /// @throws eckit::SeriousBug if 'keyword' is not found
    std::string canonicalValue(const std::string& keyword) const { return canonicalise(keyword, get(keyword)); }

    Key canonical() const;

    const TypesRegistry& registry() const { return registry_; }

private:  // members
    const TypesRegistry& registry_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

namespace std {

template<>
struct hash<fdb5::Key> {
    size_t operator()(const fdb5::Key& key) const { return std::hash<std::string>()(key.valuesToString()); }
};

}  // namespace std

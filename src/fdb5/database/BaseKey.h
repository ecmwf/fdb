/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   BaseKey.h
/// @author Metin Cakircali
/// @date   Oct 2024

#pragma once

#include "eckit/types/Types.h"

#include <cstddef>
#include <initializer_list>
#include <ostream>
#include <string>
#include <utility>  // std::pair

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class BaseKey {
public:  // types

    using pair_type              = std::pair<const std::string, std::string>;
    using value_type             = eckit::StringDict;
    using iterator               = value_type::iterator;
    using const_iterator         = value_type::const_iterator;
    using const_reverse_iterator = value_type::const_reverse_iterator;

public:  // methods

    explicit BaseKey(value_type keys) : keys_{std::move(keys)} {
        /// @note the order of keys (in map<>) is not "insertion order"
        for (const auto& key : *this) {
            names_.emplace_back(key.first);
        }
    }

    BaseKey(std::initializer_list<pair_type> keys) : keys_(keys) {
        for (const auto& key : *this) {
            names_.emplace_back(key.first);
        }
    }

    explicit BaseKey(eckit::Stream& stream) { decode(stream); }

    // RULES

    BaseKey()                                = default;
    BaseKey(const BaseKey& other)            = default;
    BaseKey& operator=(const BaseKey& other) = default;
    BaseKey(BaseKey&& other)                 = default;
    BaseKey& operator=(BaseKey&& other)      = default;
    virtual ~BaseKey()                       = default;

    virtual std::string type() const = 0;

    // ITERATORS

    iterator begin() noexcept { return keys_.begin(); }

    const_iterator begin() const noexcept { return keys_.begin(); }

    iterator end() noexcept { return keys_.end(); }

    const_iterator end() const noexcept { return keys_.end(); }

    const_reverse_iterator rbegin() const noexcept { return keys_.rbegin(); }

    const_reverse_iterator rend() const noexcept { return keys_.rend(); }

    std::pair<const_iterator, bool> find(const std::string& keyword) const {
        const auto iter = keys_.find(keyword);
        return {iter, iter != end()};
    }

    // ACCESSORS

    bool empty() const noexcept { return keys_.empty(); }

    std::size_t size() const noexcept { return keys_.size(); }

    /// @throws eckit::SeriousBug if 'keyword' is not found
    const std::string& get(const std::string& keyword) const;

    /// @note returns a copy
    /// @throws eckit::SeriousBug if 'keyword' is not found
    std::string value(const std::string& keyword) const { return get(keyword); }

    const eckit::StringDict& keyDict() const { return keys_; }

    eckit::StringSet keys() const;

    metkit::mars::MarsRequest request(const std::string& verb = "retrieve") const;

    const eckit::StringList& names() const { return names_; }

    // MODIFIERS

    void clear();

    void set(const std::string& keyword, const std::string& value);

    void unset(const std::string& keyword);

    void push(const std::string& keyword, const std::string& value);

    void pop(const std::string& keyword);

    void pushFrom(const BaseKey& other);

    void popFrom(const BaseKey& other);

    // OPERATORS

    const std::string& operator[](const std::string& keyword) const { return get(keyword); }

    bool operator<(const BaseKey& other) const { return keys_ < other.keys_; }

    bool operator!=(const BaseKey& other) const { return keys_ != other.keys_; }

    bool operator==(const BaseKey& other) const { return keys_ == other.keys_; }

    friend std::ostream& operator<<(std::ostream& stream, const BaseKey& key) {
        key.print(stream);
        return stream;
    }

    friend eckit::Stream& operator>>(eckit::Stream& stream, BaseKey& key) {
        key.decode(stream);
        return stream;
    }

    friend eckit::Stream& operator<<(eckit::Stream& stream, const BaseKey& key) {
        key.encode(stream);
        return stream;
    }

    operator std::string() const { return toString(); }

    /// @note same as keyDict but throws when value.empty()
    operator eckit::StringDict() const;

    size_t encodeSize() const;

protected:  // methods

    void decode(eckit::Stream& stream);

    void encode(eckit::Stream& stream) const;

private:  // methods

    std::string toString() const;

    void print(std::ostream& out) const;

private:  // members

    value_type keys_;

    eckit::StringList names_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

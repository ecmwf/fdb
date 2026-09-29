/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/BaseKey.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/log/CodeLocation.h"
#include "eckit/serialisation/Stream.h"
#include "eckit/types/Types.h"
#include "eckit/utils/StringTools.h"

#include "metkit/mars/MarsRequest.h"

#include <cstddef>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
// HELPERS

namespace {

class ReverseName {
    using value_type = eckit::StringList;

public:  // methods

    ReverseName() = delete;
    ReverseName(const ReverseName&) = delete;
    ReverseName& operator=(const ReverseName&) = delete;
    ReverseName(ReverseName&&) = delete;
    ReverseName& operator=(ReverseName&&) = delete;
    ~ReverseName() = default;

    explicit ReverseName(const value_type& value) : value_{value} {}

    auto begin() const -> value_type::const_reverse_iterator { return value_.rbegin(); }

    auto end() const -> value_type::const_reverse_iterator { return value_.rend(); }

private:  // members

    const value_type& value_;
};

}  // namespace

//----------------------------------------------------------------------------------------------------------------------
// ACCESSORS

const std::string& BaseKey::get(const std::string& keyword) const {

    if (const auto [iter, found] = find(keyword); found) {
        return iter->second;
    }

    std::ostringstream oss;
    oss << "Could not find [" + keyword + "] in " << *this;
    throw eckit::SeriousBug(oss.str(), Here());
}

eckit::StringSet BaseKey::keys() const {
    eckit::StringSet result;
    for (const auto& pair : *this) {
        result.insert(pair.first);
    }
    return result;
}

metkit::mars::MarsRequest BaseKey::request(const std::string& verb) const {
    return {verb, keys_};
}

//----------------------------------------------------------------------------------------------------------------------
// MODIFIERS

void BaseKey::clear() {
    keys_.clear();
    names_.clear();
}

void BaseKey::set(const std::string& keyword, const std::string& value) {
    /// @note this unfortunate (consequence of insertion-order problem) check is not fully safe
    ASSERT(names_.size() == keys_.size());

    if (const auto iter = keys_.find(keyword); iter != keys_.end()) {
        iter->second = eckit::StringTools::lower(value);
    }
    else {
        names_.push_back(keyword);
        keys_[keyword] = eckit::StringTools::lower(value);
    }
}

void BaseKey::unset(const std::string& keyword) {
    keys_.erase(keyword);
}

void BaseKey::push(const std::string& keyword, const std::string& value) {
    keys_[keyword] = value;
    names_.push_back(keyword);
}

void BaseKey::pop(const std::string& keyword) {
    keys_.erase(keyword);
    ASSERT(names_.back() == keyword);
    names_.pop_back();
}

void BaseKey::pushFrom(const BaseKey& other) {
    for (const auto& keyword : other.names()) {
        const auto& value = other.get(keyword);
        push(keyword, value);
    }
}

void BaseKey::popFrom(const BaseKey& other) {
    for (const auto& keyword : ReverseName(other.names())) {
        pop(keyword);
    }
}

//----------------------------------------------------------------------------------------------------------------------

void BaseKey::decode(eckit::Stream& stream) {

    clear();

    std::size_t size = 0;
    std::string keyword;
    std::string value;

    stream >> size;
    for (std::size_t i = 0; i < size; ++i) {
        stream >> keyword;
        stream >> value;
        keys_[keyword] = eckit::StringTools::lower(value);
    }

    stream >> size;
    names_.reserve(size);
    for (std::size_t i = 0; i < size; ++i) {
        stream >> keyword;
        stream >> value;
        names_.push_back(keyword);
    }
}

void BaseKey::encode(eckit::Stream& stream) const {
    stream << keys_.size();
    for (const auto& [keyword, value] : *this) {
        stream << keyword << value;
    }
    stream << names_.size();
    for (const auto& keyword : names_) {
        stream << keyword << "";
    }  // << type(keyword)
}

size_t encodeString(const std::string& str) {
    return (1 + 4 + str.length());
}

size_t BaseKey::encodeSize() const {
    size_t size = 1 + 4;
    for (const auto& [keyword, value] : keys_) {
        size += encodeString(keyword) + encodeString(value);
    }
    size += (1 + 4);
    for (const auto& keyword : names_) {
        size += encodeString(keyword) + encodeString("");
    }
    return size;
}

//----------------------------------------------------------------------------------------------------------------------

BaseKey::operator eckit::StringDict() const {
    /// @note this unfortunate (consequence of insertion-order problem) check is not fully safe
    ASSERT(names_.size() == keys_.size());

    eckit::StringDict result;
    for (const auto& keyword : names()) {
        const auto& value = get(keyword);
        ASSERT(!value.empty());
        result[keyword] = value;
    }

    return result;
}

std::string BaseKey::toString() const {
    std::string result;

    const char* sep = "";
    for (const auto& keyword : names()) {
        const auto& value = get(keyword);
        if (!value.empty()) {
            result += sep + keyword + '=' + value;
            sep = ",";
        }
    }

    return result;
}

void BaseKey::print(std::ostream& out) const {
    /// @note this unfortunate (consequence of insertion-order problem) check is not fully safe
    if (names_.size() == size()) {
        out << "{" << toString() << "}";
    }
    else {
        out << keys_;
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

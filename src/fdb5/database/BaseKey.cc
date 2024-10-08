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

#include "metkit/mars/MarsRequest.h"

#include <algorithm>
#include <cstddef>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
// HELPERS

namespace {

class Reverse {
public:
    explicit Reverse(const BaseKey& key) : key_ {key} { }

    BaseKey::const_reverse_iterator begin() const { return key_.rbegin(); }

    BaseKey::const_reverse_iterator end() const { return key_.rend(); }

private:
    const BaseKey& key_;
};

template<typename T>
auto findKeyword(T& key, const std::string& keyword) noexcept {
    auto pred = [&keyword](const auto& pair) { return pair.first == keyword; };
    return std::find_if(key.begin(), key.end(), pred);
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------
// BASE KEY

auto BaseKey::find(const std::string& keyword) noexcept -> std::pair<iterator, bool> {
    const auto iter = findKeyword(*this, keyword);
    return {iter, iter != end()};
}

auto BaseKey::find(const std::string& keyword) const noexcept -> std::pair<const_iterator, bool> {
    const auto iter = findKeyword(*this, keyword);
    return {iter, iter != end()};
}

const std::string& BaseKey::get(const std::string& keyword) const {

    if (const auto [iter, found] = find(keyword); found) { return iter->second; }

    std::ostringstream oss;
    oss << "Could not find [" + keyword + "] in " << *this;
    throw eckit::SeriousBug(oss.str(), Here());
}

eckit::StringDict BaseKey::keyDict() const {
    eckit::StringDict result;
    for (const auto& [keyword, value] : *this) { result.emplace(keyword, value); }
    return result;
}

eckit::StringSet BaseKey::keys() const {
    eckit::StringSet result;
    for (const auto& pair : *this) { result.insert(pair.first); }
    return result;
}

metkit::mars::MarsRequest BaseKey::request(const std::string& verb) const {
    return {verb, keys_};
}

void BaseKey::pushFrom(const BaseKey& other) {
    for (const auto& [keyword, value] : other) { push(keyword, value); }
}

void BaseKey::popFrom(const BaseKey& other) {
    for (const auto& [keyword, value] : other) { pop(keyword); }
}

//----------------------------------------------------------------------------------------------------------------------

void BaseKey::encode(eckit::Stream& stream) const {
    stream << size();
    for (const auto& [keyword, value] : *this) { stream << keyword << value; }
}

void BaseKey::decode(eckit::Stream& stream) {

    clear();

    std::size_t size = 0;
    std::string keyword;
    std::string value;

    stream >> size;
    for (std::size_t i = 0; i < size; ++i) {
        stream >> keyword;
        stream >> value;
        emplace(keyword, value);
    }

    // need type here ?
}

//----------------------------------------------------------------------------------------------------------------------

BaseKey::operator eckit::StringDict() const {
    eckit::StringDict result;
    /// @note canonical values
    for (const auto& [keyword, value] : *this) {
        ASSERT(!value.empty());
        result[keyword] = value;
    }
    return result;
}

std::string BaseKey::toString() const {
    std::string result;
    const char* sep = "";
    for (const auto& [keyword, value] : *this) {
        if (!value.empty()) {
            result += sep + keyword + '=' + value;
            sep     = ",";
        }
    }
    return result;
}

void BaseKey::print(std::ostream& out) const {
    out << "{" << toString() << "}";
}

//----------------------------------------------------------------------------------------------------------------------

bool BaseKey::match(const BaseKey& other) const {

    for (const auto& [keyword, value] : other) {

        const auto [iter, found] = find(keyword);

        if (!found) { return false; }

        if (value != iter->second && !value.empty()) { return false; }
    }

    return true;
}

bool BaseKey::match(const metkit::mars::MarsRequest& request) const {

    for (const auto& param : request.parameters()) {

        const auto [iter, found] = find(param.name());

        if (!found) { return false; }

        const auto& values = param.values();

        if (std::find(values.begin(), values.end(), iter->second) == values.end()) { return false; }
    }

    return true;
}

bool BaseKey::partialMatch(const metkit::mars::MarsRequest& request) const {

    // if (request.parameters().empty()) { return true; }

    for (const auto& [keyword, value] : *this) {

        const auto& values = request.values(keyword, /* emptyOk */ true);

        if (values.empty()) { continue; }

        if (std::find(values.begin(), values.end(), value) == values.end()) { return false; }
    }

    return true;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/database/Key.h"

#include "fdb5/database/BaseKey.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/types/Type.h"

#include "eckit/container/DenseSet.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/types/Types.h"
#include "eckit/utils/Tokenizer.h"

#include <cstddef>
#include <ostream>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>

//----------------------------------------------------------------------------------------------------------------------
// HELPERS

namespace {

template<typename T>
std::string valuesToString(const T& key) {

    static_assert(std::is_base_of_v<fdb5::BaseKey, T>, "valuesToString() only works with types based on BaseKey");

    std::ostringstream oss;

    const char* sep = "";
    for (const auto& [keyword, value] : key) {
        if constexpr (std::is_same_v<T, fdb5::TypedKey>) {
            oss << sep << key.canonicalise(keyword, value);
        } else {
            oss << sep << value;  // canonical
        }
        sep = ":";
    }

    return oss.str();
}

}  // namespace

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
// KEY

Key Key::parse(const std::string& keyString) {

    Key key;

    for (const auto& bit : eckit::Tokenizer(",").tokenize(keyString)) {
        const auto pair = eckit::Tokenizer("=").tokenize(bit);
        ASSERT(pair.size() == 2);
        key.emplace(pair[0], pair[1]);
    }

    return key;
}

std::string Key::valuesToString() const {
    return ::valuesToString(*this);
}

void Key::validateKeys(const Key& other, bool checkAlsoValues) const {

    eckit::StringSet missing;
    eckit::StringSet mismatch;

    for (const auto& [keyword, value] : other) {
        if (const auto [iter, found] = find(keyword); found) {
            if (checkAlsoValues && value != iter->second) {
                mismatch.insert(keyword + '=' + value + " and " + iter->second);
            }
        } else {
            missing.insert(keyword);
        }
    }

    if (missing.size() || mismatch.size()) {
        std::ostringstream oss;
        if (missing.size()) { oss << "Keywords not used: " << missing << " "; }
        if (mismatch.size()) { oss << "Values mismatch: " << mismatch << " "; }
        oss << "for key=" << *this << " validating against=" << other;
        throw eckit::SeriousBug(oss.str());
    }
}

bool Key::matchValues(const std::string& keyword, const eckit::DenseSet<std::string>& values) const {

    if (const auto [iter, found] = find(keyword); found) { return values.find(iter->second) != values.end(); }

    return false;
}

//----------------------------------------------------------------------------------------------------------------------
// TYPED KEY

// TypedKey TypedKey::parse(const std::string& keyString, const TypesRegistry& reg) {
//
//     auto key = Key::parse(keyString);
//
//     for (auto& [keyword, value] : key) { value = reg.lookupType(keyword).tidy(value); }
//
//     return {std::move(key), reg};
// }

std::string TypedKey::valuesToString() const {
    return ::valuesToString(*this);
}

std::string TypedKey::canonicalise(const std::string& keyword, const std::string& value) const {
    return value.empty() ? value : registry().lookupType(keyword).toKey(value);
}

Key TypedKey::canonical() const {
    Key key;
    for (const auto& [keyword, value] : *this) { key.emplace(keyword, canonicalise(keyword, value)); }
    return key;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

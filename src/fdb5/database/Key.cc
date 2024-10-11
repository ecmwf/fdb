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
#include "metkit/mars/MarsRequest.h"

#include <algorithm>
#include <cstddef>
#include <ostream>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>

//----------------------------------------------------------------------------------------------------------------------
// HELPERS

namespace {

template<typename T, typename = std::enable_if_t<std::is_base_of_v<fdb5::BaseKey, T>>>
std::string valuesToString(const T& key) {

    std::ostringstream oss;

    /// @note this unfortunate (consequence of insertion-order problem) check is not fully safe
    if (key.names().size() != key.size()) {
        oss << "names and keys size mismatch" << '\n'
            << "    names: " << key.names().size() << "  " << key.names() << '\n'
            << "    keys:  " << key.size() << "  " << key.keyDict() << '\n';
        throw eckit::SeriousBug(oss.str());
    }

    const char* sep = "";
    for (const auto& keyword : key.names()) {
        if constexpr (std::is_same_v<T, fdb5::Key>) {
            oss << sep << key[keyword];
        } else {
            static_assert(false, "valuesToString() is not yet implemented for this type");
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
        key.push(pair[0], pair[1]);
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

//----------------------------------------------------------------------------------------------------------------------
// MATCH

bool Key::match(const Key& other) const {

    for (const auto& [keyword, value] : other) {

        if (const auto [iter, found] = find(keyword); found) {
            if (iter->second == value && !value.empty()) { continue; }
        }

        return false;
    }

    return true;
}

bool Key::match(const metkit::mars::MarsRequest& request) const {

    // for (const auto& param : request.parameters()) {
    for (const auto& param : request.params()) {

        if (auto [iter, found] = find(param); found) {
            const auto& values = request.values(param);
            if (std::find(values.begin(), values.end(), iter->second) != values.end()) { continue; }
        }

        return false;
    }

    return true;
}

bool Key::partialMatch(const metkit::mars::MarsRequest& request) const {

    for (const auto& [keyword, value] : *this) {

        const auto& values = request.values(keyword, /* emptyOk */ true);

        if (values.empty()) { continue; }

        if (std::find(values.begin(), values.end(), value) == values.end()) { return false; }
    }

    return true;
}

bool Key::matchValues(const std::string& keyword, const eckit::DenseSet<std::string>& values) const {

    if (const auto [iter, found] = find(keyword); found) { return values.find(iter->second) != values.end(); }

    return false;
}

//----------------------------------------------------------------------------------------------------------------------
// TYPED KEY

Key TypedKey::canonical() const {
    Key key;
    for (const auto& keyword : names()) {
        const auto& value = get(keyword);
        value.empty() ? key.push(keyword, value) : key.push(keyword, registry_.lookupType(keyword).toKey(value));
    }
    return key;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

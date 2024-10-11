/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   KeyChain.h
/// @author Metin Cakircali
/// @date   Aug 2024

#pragma once

#include "eckit/serialisation/Stream.h"
#include "fdb5/database/Key.h"

#include <array>
#include <optional>
#include <ostream>
#include <utility>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
/// @brief KeyChain is a set of 3-level keys, where each level is a Key object.
/// Level 1 is the database key, level 2 is the index key, and level 3 is the datum key.

class KeyChain: public std::array<Key, 3> {
    using ParentType = std::array<Key, 3>;

public:  // methods
    KeyChain() = default;

    KeyChain(Key&& dbKey, Key&& indexKey, Key&& datumKey):
        ParentType {std::move(dbKey), std::move(indexKey), std::move(datumKey)} { }

    KeyChain(const Key& dbKey, const Key& indexKey, const Key& datumKey): ParentType {dbKey, indexKey, datumKey} { }

    KeyChain(Key&& dbKey, Key&& indexKey): ParentType {std::move(dbKey), std::move(indexKey), Key {}} { }

    KeyChain(const Key& dbKey, const Key& indexKey): ParentType {dbKey, indexKey, Key {}} { }

    explicit KeyChain(Key&& dbKey): ParentType {std::move(dbKey), Key {}, Key {}} { }

    explicit KeyChain(const Key& dbKey): ParentType {dbKey, Key {}, Key {}} { }

    const Key& combine() const {
        if (!key_) {
            key_ = std::make_optional<Key>();
            for (const auto& key : *this) {
                for (const auto& [keyword, value] : key) { key_->set(keyword, value); }
            }
        }
        return *key_;
    }

private:  // methods
    void print(std::ostream& out) const {
        for (auto&& key : *this) {
            if (!key.empty()) { out << key; }
        }
    }

    friend std::ostream& operator<<(std::ostream& out, const KeyChain& keys) {
        keys.print(out);
        return out;
    }

    friend eckit::Stream& operator<<(eckit::Stream& out, const KeyChain& keys) {
        for (auto&& key : keys) { out << key; }
        return out;
    }

    friend eckit::Stream& operator>>(eckit::Stream& out, KeyChain& keys) {
        for (auto&& key : keys) { out >> key; }
        return out;
    }

private:  // members
    mutable std::optional<Key> key_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

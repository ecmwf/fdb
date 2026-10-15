// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   TypesRegistry.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#pragma once

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "eckit/serialisation/Streamable.h"

#include "fdb5/types/Type.h"

namespace metkit::mars {
class MarsRequest;
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class TypesRegistry : public eckit::Streamable {

public:  // methods

    TypesRegistry() = default;

    explicit TypesRegistry(eckit::Stream& stream);

    void decode(eckit::Stream& stream);
    void encode(eckit::Stream& out) const override;

    const Type& lookupType(const std::string& keyword) const;

    void addType(const std::string&, const std::string&);
    void updateParent(const TypesRegistry& parent);
    void dump(std::ostream& out) const;
    void dump(std::ostream& out, const std::string& keyword) const;

    metkit::mars::MarsRequest canonicalise(const metkit::mars::MarsRequest& request) const;

    // streamable

    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

    std::size_t hash() const;

    bool operator==(const TypesRegistry& other) const;

private:  // methods

    void print(std::ostream& out) const;

    friend std::ostream& operator<<(std::ostream& s, const TypesRegistry& x);

private:  // members

    std::map<std::string, std::string> types_;

    const TypesRegistry* parent_{nullptr};

    using TypeMap = std::map<std::string, std::unique_ptr<const Type>>;
    mutable std::mutex cacheMutex_;
    mutable TypeMap cache_;

    // streamable

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<TypesRegistry> reanimator_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

template <>
struct std::hash<const fdb5::TypesRegistry*> {
    std::size_t operator()(const fdb5::TypesRegistry* registry) const { return registry->hash(); }
};

template <>
struct std::equal_to<const fdb5::TypesRegistry*> {
    bool operator()(const fdb5::TypesRegistry* left, const fdb5::TypesRegistry* right) const { return *left == *right; }
};


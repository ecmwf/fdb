/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TypesRegistry.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_TypesRegistry_H
#define fdb5_TypesRegistry_H

#include <map>
#include <memory>
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

    const Type& lookupType(const std::string& keyword) const;

    void addType(const std::string&, const std::string&);
    void updateParent(const TypesRegistry& parent);
    void dump(std::ostream& out) const;
    void dump(std::ostream& out, const std::string& keyword) const;

    metkit::mars::MarsRequest canonicalise(const metkit::mars::MarsRequest& request) const;

    // streamable

    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

private:  // methods
    void encode(eckit::Stream& out) const override;

    void print(std::ostream& out) const;

    friend std::ostream& operator<<(std::ostream& s, const TypesRegistry& x);

private:  // members
    std::map<std::string, std::string> types_;

    const TypesRegistry* parent_ {nullptr};

    using TypeMap = std::map<std::string, std::unique_ptr<const Type>>;
    mutable TypeMap cache_;

    // streamable

    static eckit::ClassSpec                 classSpec_;
    static eckit::Reanimator<TypesRegistry> reanimator_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

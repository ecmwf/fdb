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

#include <string>
#include <map>
#include <memory>
#include <optional>
#include <functional>

#include "eckit/serialisation/Streamable.h"

namespace fdb5 {

class Type;

//----------------------------------------------------------------------------------------------------------------------

class TypesRegistry : public eckit::Streamable  {

public: // methods

    TypesRegistry();
    TypesRegistry(eckit::Stream& s);

    ~TypesRegistry();

    const Type &lookupType(const std::string &keyword) const;

    void addType(const std::string &, const std::string &);
    void updateParent(const TypesRegistry& parent);
    void dump( std::ostream &out ) const;
    void dump( std::ostream &out, const std::string &keyword ) const;

	const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }
	static const eckit::ClassSpec&  classSpec() { return classSpec_; }
    void encode(eckit::Stream& s) const override;

private: // members

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<TypesRegistry> reanimator_;

    typedef std::map<std::string, Type *> TypeMap;

    mutable TypeMap cache_;

    std::map<std::string, std::string> types_;
    std::optional<std::reference_wrapper<const TypesRegistry>> parent_;

    friend std::ostream &operator<<(std::ostream &s, const TypesRegistry &x);


    void print( std::ostream &out ) const;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

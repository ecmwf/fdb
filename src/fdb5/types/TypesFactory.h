// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   TypesFactory.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#pragma once

#include <iosfwd>
#include <string>

namespace fdb5 {

class Type;

//----------------------------------------------------------------------------------------------------------------------

/// A self-registering factory for producing TypesFactory instances

class TypesFactory {

    virtual Type* make(const std::string& keyword) const = 0;

protected:

    TypesFactory(const std::string&);
    virtual ~TypesFactory();

    std::string name_;

public:

    static void list(std::ostream&);
    static Type* build(const std::string& name, const std::string& keyword);
};

/// Templated specialisation of the self-registering factory,
/// that does the self-registration, and the construction of each object.

template <class T>
class TypeBuilder : public TypesFactory {

    Type* make(const std::string& keyword) const override { return new T(keyword, name_); }

public:

    TypeBuilder(const std::string& name) : TypesFactory(name) {}
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5


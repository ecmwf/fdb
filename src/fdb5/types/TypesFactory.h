/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   TypesFactory.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_TypesFactory_H
#define fdb5_TypesFactory_H

#include <string>

#include "eckit/memory/NonCopyable.h"
#include "eckit/types/Types.h"

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

#endif

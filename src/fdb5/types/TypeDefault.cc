/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/types/TypeDefault.h"
#include "fdb5/types/TypesFactory.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeDefault::TypeDefault(const std::string& name, const std::string& type) : Type(name, type) {}

TypeDefault::~TypeDefault() {}

void TypeDefault::print(std::ostream& out) const {
    out << "TypeDefault[name=" << name_ << "]";
}

static TypeBuilder<TypeDefault> type("Default");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

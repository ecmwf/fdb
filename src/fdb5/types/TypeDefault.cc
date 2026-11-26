// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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

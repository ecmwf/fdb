/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/types/TypesFactory.h"
#include "fdb5/types/TypeLowercase.h"

#include <algorithm>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeLowercase::TypeLowercase(const std::string &name, const std::string &type) :
    Type(name, type) {
}

TypeLowercase::~TypeLowercase() {
}

void TypeLowercase::print(std::ostream &out) const {
    out << "TypeLowercase[name=" << name_ << "]";
}

std::string TypeLowercase::tidy(const std::string &keyword, const std::string &value) const {
    std::string out;
    out.resize(value.size());

    std::transform(value.begin(), value.end(), out.begin(), [](unsigned char c){ return std::tolower(c); });

    return out;
}

std::string TypeLowercase::toKey(const std::string &keyword, const std::string &value) const {
    return tidy(keyword, value);
}

static TypeBuilder<TypeLowercase> type("Lowercase");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

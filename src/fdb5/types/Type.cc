/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/types/Type.h"
#include "marslib/MarsRequest.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Type::Type(const std::string &name, const std::string &type) :
    name_(name),
    type_(type) {
}

Type::~Type() {
}

void Type::getValues(const MarsRequest &request,
                     const std::string &keyword,
                     eckit::StringList &values,
                     const NotifyWind&,
                     const DB*) const {
    request.getValues(keyword, values);
}

const std::string &Type::type() const {
    return type_;
}

std::string Type::tidy(const std::string&,
                       const std::string &value) const  {
    return value;
}

void Type::toKey(std::ostream &out,
                 const std::string&,
                 const std::string &value) const {
    out << value;
}

std::ostream &operator<<(std::ostream &s, const Type &x) {
    x.print(s);
    return s;
}

bool Type::match(const std::string&, const std::string& value1, const std::string& value2) const
{
    return (value1 == value2);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

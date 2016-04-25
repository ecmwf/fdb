/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "marslib/MarsTask.h"

#include "fdb5/Type.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Type::Type(const std::string& name, const std::string& type) :
    name_(name),
    type_(type)
{
}

Type::~Type() {
}

void Type::getValues(const MarsRequest& request,
                               const std::string& keyword,
                               StringList& values,
                               const MarsTask& task,
                               const DB* db) const
{
    request.getValues(keyword, values);
}


void Type::toKey(std::ostream& out,
                       const std::string& keyword,
                       const std::string& value) const {
    out << value;
}

std::ostream& operator<<(std::ostream& s, const Type& x)
{
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
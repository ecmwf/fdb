/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/parser/StringTools.h"
#include "marslib/MarsRequest.h"

#include "fdb5/type/TypesFactory.h"
#include "fdb5/type/TypeGrid.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeGrid::TypeGrid(const std::string &name, const std::string &type) :
    Type(name, type) {
}

TypeGrid::~TypeGrid() {
}

void TypeGrid::toKey(std::ostream &out,
                     const std::string &keyword,
                     const std::string &value) const {

    std::string s(value);
    std::replace( s.begin(), s.end(), '/', '+');
    out << s;
}

void TypeGrid::getValues(const MarsRequest &request,
                         const std::string &keyword,
                         eckit::StringList &values,
                         const MarsTask &task,
                         const DB *db) const {
    std::vector<std::string> v;
    request.getValues(keyword, v);
    values.push_back(eckit::StringTools::join("+", v));
}


void TypeGrid::print(std::ostream &out) const {
    out << "TypeGrid[name=" << name_ << "]";
}

static TypeBuilder<TypeGrid> type("Grid");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>

#include "eckit/utils/StringTools.h"
#include "metkit/mars/MarsRequest.h"

#include "fdb5/types/TypesFactory.h"
#include "fdb5/types/TypeGrid.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeGrid::TypeGrid(const std::string &name, const std::string &type) :
    Type(name, type) {
}

TypeGrid::~TypeGrid() {
}

std::string TypeGrid::toKey(const std::string& value) const {

    std::string s(value);
    std::replace( s.begin(), s.end(), '/', '+');
    return s;
}

void TypeGrid::getValues(const metkit::mars::MarsRequest& request,
                         const std::string& keyword,
                         eckit::StringList& values,
                         const Notifier&,
                         const DB*) const {
    std::vector<std::string> v;
    request.getValues(keyword, v, true);
    values.push_back(eckit::StringTools::join("+", v));
}


void TypeGrid::print(std::ostream &out) const {
    out << "TypeGrid[name=" << name_ << "]";
}

static TypeBuilder<TypeGrid> type("Grid");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

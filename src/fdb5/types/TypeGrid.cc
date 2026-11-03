// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include <algorithm>

#include "eckit/utils/StringTools.h"
#include "metkit/mars/MarsRequest.h"

#include "fdb5/types/TypeGrid.h"
#include "fdb5/types/TypesFactory.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeGrid::TypeGrid(const std::string& name, const std::string& type) : Type(name, type) {}

TypeGrid::~TypeGrid() {}

std::string TypeGrid::toKey(const std::string& value) const {

    std::string s(value);
    std::replace(s.begin(), s.end(), '/', '+');
    return s;
}

void TypeGrid::getValues(const metkit::mars::MarsRequest& request, const std::string& keyword,
                         eckit::StringList& values, const CatalogueReader*) const {
    std::vector<std::string> v;
    request.getValues(keyword, v, true);
    values.push_back(eckit::StringTools::join("+", v));
}


void TypeGrid::print(std::ostream& out) const {
    out << "TypeGrid[name=" << name_ << "]";
}

static TypeBuilder<TypeGrid> type("Grid");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

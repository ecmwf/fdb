// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/types/TypeAbbreviation.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/types/TypesFactory.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeAbbreviation::TypeAbbreviation(const std::string& name, const std::string& type) : Type(name, type) {
    count_ = char(type[type.length() - 1]) - '0';
}

TypeAbbreviation::~TypeAbbreviation() {}

std::string TypeAbbreviation::toKey(const std::string& value) const {

    return value.substr(0, count_);
}

void TypeAbbreviation::getValues(const metkit::mars::MarsRequest& request, const std::string& keyword,
                                 eckit::StringList& values, const CatalogueReader*) const {
    std::vector<std::string> vals;

    request.getValues(keyword, vals, true);

    values.reserve(vals.size());

    for (std::vector<std::string>::const_iterator i = vals.begin(); i != vals.end(); ++i) {
        values.push_back((*i).substr(0, count_));
    }
}

void TypeAbbreviation::print(std::ostream& out) const {
    out << "TypeAbbreviation[name=" << name_ << ",count=" << count_ << "]";
}

static TypeBuilder<TypeAbbreviation> type("First3");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

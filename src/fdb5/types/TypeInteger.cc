// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "eckit/utils/Translator.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/types/TypeInteger.h"
#include "fdb5/types/TypesFactory.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeInteger::TypeInteger(const std::string& name, const std::string& type) : Type(name, type) {}

TypeInteger::~TypeInteger() {}

void TypeInteger::getValues(const metkit::mars::MarsRequest& request, const std::string& keyword,
                            eckit::StringList& values, const CatalogueReader*) const {
    std::vector<long> intValues;

    request.getValues(keyword, intValues, true);

    eckit::Translator<long, std::string> t;

    values.reserve(intValues.size());

    for (std::vector<long>::const_iterator i = intValues.begin(); i != intValues.end(); ++i) {
        values.push_back(t(*i));
    }
}

void TypeInteger::print(std::ostream& out) const {
    out << "TypeInteger[name=" << name_ << "]";
}

static TypeBuilder<TypeInteger> type("Integer");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

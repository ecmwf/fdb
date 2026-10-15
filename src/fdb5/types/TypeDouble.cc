// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "eckit/utils/Translator.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/types/TypeDouble.h"
#include "fdb5/types/TypesFactory.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeDouble::TypeDouble(const std::string& name, const std::string& type) : Type(name, type) {}

TypeDouble::~TypeDouble() {}

std::string TypeDouble::toKey(const std::string& value) const {
    double v = eckit::Translator<std::string, double>()(value);
    long long ll = static_cast<long long>(v);

    if (ll == v) {
        return eckit::Translator<long long, std::string>()(ll);
    }
    else {
        return eckit::Translator<double, std::string>()(v);
    }
}

void TypeDouble::getValues(const metkit::mars::MarsRequest& request, const std::string& keyword,
                           eckit::StringList& values, const CatalogueReader*) const {
    std::vector<double> dblValues;

    request.getValues(keyword, dblValues, true);

    eckit::Translator<double, std::string> t;

    values.reserve(dblValues.size());

    for (std::vector<double>::const_iterator i = dblValues.begin(); i != dblValues.end(); ++i) {
        values.push_back(t(*i));
    }
}

void TypeDouble::print(std::ostream& out) const {
    out << "TypeDouble[name=" << name_ << "]";
}

static TypeBuilder<TypeDouble> type("Double");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

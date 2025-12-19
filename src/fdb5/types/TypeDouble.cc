/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

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
                           eckit::StringList& values, const Notifier&, const CatalogueReader*) const {
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

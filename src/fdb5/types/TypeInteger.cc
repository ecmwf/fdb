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

#include "fdb5/types/TypesFactory.h"
#include "fdb5/types/TypeInteger.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeInteger::TypeInteger(const std::string &name, const std::string &type) :
    Type(name, type) {
}

TypeInteger::~TypeInteger() {
}

void TypeInteger::getValues(const metkit::mars::MarsRequest& request,
                            const std::string& keyword,
                            eckit::StringList& values,
                            const Notifier&,
                            const CatalogueReader*) const {
    std::vector<long> intValues;

    request.getValues(keyword, intValues, true);

    eckit::Translator<long, std::string> t;

    values.reserve(intValues.size());

    for (std::vector<long>::const_iterator i = intValues.begin(); i != intValues.end(); ++i) {
        values.push_back(t(*i));
    }
}

void TypeInteger::print(std::ostream &out) const {
    out << "TypeInteger[name=" << name_ << "]";
}

static TypeBuilder<TypeInteger> type("Integer");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

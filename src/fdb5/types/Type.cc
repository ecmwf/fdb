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
#include "eckit/utils/StringTools.h"
#include "metkit/mars/MarsRequest.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Type::Type(const std::string& name, const std::string& type, const std::string& alias) :
    name_(name), type_(type), alias_(alias) {}

void Type::getValues(const metkit::mars::MarsRequest& request, const std::string& keyword, eckit::StringList& values,
                     const Notifier&, const CatalogueReader*) const {
    request.getValues(keyword, values, true);
}

const std::string& Type::alias() const {
    return alias_.empty() ? name_ : alias_;
}

const std::string& Type::type() const {
    return type_;
}

std::string Type::tidy(const std::string& value) const {
    return eckit::StringTools::lower(value);
}

std::string Type::toKey(const std::string& value) const {
    return eckit::StringTools::lower(value);
}

std::ostream& operator<<(std::ostream& s, const Type& x) {
    x.print(s);
    return s;
}

bool Type::match(const std::string&, const std::string& value1, const std::string& value2) const {
    return (value1 == value2);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

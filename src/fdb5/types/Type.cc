// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/types/Type.h"
#include "eckit/utils/StringTools.h"
#include "metkit/mars/MarsRequest.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Type::Type(const std::string& name, const std::string& type, const std::string& alias) :
    name_(name), type_(type), alias_(alias) {}

void Type::getValues(const metkit::mars::MarsRequest& request, const std::string& keyword, eckit::StringList& values,
                     const CatalogueReader*) const {
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

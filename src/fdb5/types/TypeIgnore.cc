// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0


#include "fdb5/types/TypeIgnore.h"
#include "fdb5/types/TypesFactory.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeIgnore::TypeIgnore(const std::string& name, const std::string& type) : Type(name, type) {}

TypeIgnore::~TypeIgnore() {}


std::string TypeIgnore::toKey(const std::string&) const {
    return "";
}

void TypeIgnore::getValues(const metkit::mars::MarsRequest&, const std::string&, eckit::StringList&,
                           const CatalogueReader*) const {}

void TypeIgnore::print(std::ostream& out) const {
    out << "TypeIgnore[name=" << name_ << "]";
}

static TypeBuilder<TypeIgnore> type("Ignore");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

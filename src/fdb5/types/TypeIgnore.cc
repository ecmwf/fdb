/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/types/TypesFactory.h"
#include "fdb5/types/TypeIgnore.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeIgnore::TypeIgnore(const std::string &name, const std::string &type) :
    Type(name, type) {
}

TypeIgnore::~TypeIgnore() {
}


std::string TypeIgnore::toKey(const std::string&) const {
    return "";
}

void TypeIgnore::getValues(const metkit::mars::MarsRequest&,
                           const std::string&,
                           eckit::StringList&,
                           const Notifier&,
                           const DB*) const {
}

void TypeIgnore::print(std::ostream &out) const {
    out << "TypeIgnore[name=" << name_ << "]";
}

static TypeBuilder<TypeIgnore> type("Ignore");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

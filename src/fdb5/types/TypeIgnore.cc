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


void TypeIgnore::toKey(std::ostream&,
                       const std::string&,
                       const std::string&) const {
}

void TypeIgnore::getValues(const MarsRequest&,
                           const std::string&,
                           eckit::StringList&,
                           const NotifyWind&,
                           const DB*) const {
}

void TypeIgnore::print(std::ostream &out) const {
    out << "TypeIgnore[name=" << name_ << "]";
}

static TypeBuilder<TypeIgnore> type("Ignore");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

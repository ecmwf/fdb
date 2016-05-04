/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/utils/Translator.h"

#include "eckit/types/Date.h"
#include "marslib/MarsRequest.h"

#include "fdb5/types/TypesFactory.h"
#include "fdb5/types/TypeExpver.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeExpver::TypeExpver(const std::string &name, const std::string &type) :
    Type(name, type) {
}

TypeExpver::~TypeExpver() {
}


void TypeExpver::toKey(std::ostream &out,
                       const std::string &keyword,
                       const std::string &value) const {

    out << std::setfill('0') << std::setw(4) << value << std::setfill(' ');

}


void TypeExpver::getValues(const MarsRequest &request,
                           const std::string &keyword,
                           eckit::StringList &values,
                           const MarsTask &task,
                           const DB *db) const {
    std::vector<std::string> expver;

    request.getValues(keyword, expver);

    values.reserve(expver.size());

    for (std::vector<std::string>::const_iterator i = expver.begin(); i != expver.end(); ++i) {
        std::ostringstream oss;
        oss << std::setfill('0') << std::setw(4) << *i;
        values.push_back(oss.str());
    }
}

void TypeExpver::print(std::ostream &out) const {
    out << "TypeExpver[name=" << name_ << "]";
}

static TypeBuilder<TypeExpver> type("Expver");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

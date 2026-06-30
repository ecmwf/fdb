/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <ostream>

#include "fdb5/database/Key.h"
#include "fdb5/rules/ExcludeAll.h"
#include "fdb5/types/TypesRegistry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

eckit::ClassSpec ExcludeAll::classSpec_ = {
    &Matcher::classSpec(),
    "ExcludeAll",
};

eckit::Reanimator<ExcludeAll> ExcludeAll::reanimator_;


ExcludeAll::ExcludeAll(const std::set<std::string>& values) : Matcher(), values_(values) {}

ExcludeAll::ExcludeAll(eckit::Stream& s) : Matcher() {

    size_t numValues;
    std::string value;

    s >> numValues;
    for (size_t i = 0; i < numValues; i++) {
        s >> value;
        values_.insert(value);
    }
}

void ExcludeAll::encode(eckit::Stream& s) const {
    s << values_.size();
    for (const std::string& value : values_) {
        s << value;
    }
}

bool ExcludeAll::match(const std::string& keyword, const Key& key) const {

    if (const auto [iter, found] = key.find(keyword); found) {
        if (!match(iter->second)) {
            return false;
        }
    }

    return true;
}

bool ExcludeAll::match(const std::string& value) const {
    return (values_.find(value) == values_.end());
}

void ExcludeAll::dump(std::ostream& s, const std::string& keyword, const TypesRegistry& registry) const {
    const char* sep = "";
    registry.dump(s, keyword);
    s << "=";
    for (std::set<std::string>::const_iterator i = values_.begin(); i != values_.end(); ++i) {
        s << sep << *i;
        sep = "/";
    }
}

void ExcludeAll::print(std::ostream& out) const {
    out << "ExcludeAll[values=";
    const char* sep = "";
    for (std::set<std::string>::const_iterator i = values_.begin(); i != values_.end(); ++i) {
        out << sep << *i;
        sep = ",";
    }
    out << "]";
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

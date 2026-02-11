/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>

#include "metkit/mars/Param.h"
#include "metkit/mars/ParamID.h"

#include "fdb5/database/Catalogue.h"
#include "fdb5/database/Notifier.h"
#include "fdb5/types/TypeParam.h"
#include "fdb5/types/TypesFactory.h"

using metkit::Param;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeParam::TypeParam(const std::string& name, const std::string& type) : Type(name, type) {}

TypeParam::~TypeParam() {}

void TypeParam::getValues(const metkit::mars::MarsRequest& request, const std::string& keyword,
                          eckit::StringList& values, const Notifier& wind, const CatalogueReader* cat) const {
    ASSERT(cat);

    auto ax = cat->axis(keyword);

    eckit::StringList us;

    Type::getValues(request, keyword, us, wind, cat);

    std::vector<Param> user;
    std::copy(us.begin(), us.end(), std::back_inserter(user));

    std::vector<Param> axis;
    if (ax) {
        std::copy(ax->get().begin(), ax->get().end(), std::back_inserter(axis));
        std::sort(axis.begin(), axis.end());
    }

    bool windConversion = false;
    metkit::ParamID::normalise(request, user, axis, windConversion);

    std::set<Param> axisSet;

    for (std::vector<Param>::const_iterator i = axis.begin(); i != axis.end(); ++i) {
        axisSet.insert(*i);
    }

    for (std::vector<Param>::const_iterator i = user.begin(); i != user.end(); ++i) {
        if (axisSet.find(*i) != axisSet.end()) {
            values.push_back(*i);
        }
    }

    // Log::info() << "TypeParam before: " << us << std::endl;
    // Log::info() << "              after: " << values << std::endl;
    // Log::info() << "               wind: " << (windConversion ? "true" : "false") << std::endl;
    // Log::info() << "               axis: " << ax << std::endl;

    if (windConversion) {
        wind.notifyWind();
    }
}

bool TypeParam::match(const std::string&, const std::string& value1, const std::string& value2) const {
    if (value1 == value2) {
        return true;
    }

    Param p1(value1);
    Param p2(value2);

    if ((p1.value() == p2.value()) && (p1.table() == 0 || p2.table() == 0)) {
        return true;
    }

    if (p1.table() * 1000 + p1.value() == p2.value()) {
        return true;
    }

    if (p2.table() * 1000 + p2.value() == p1.value()) {
        return true;
    }

    return false;
}

void TypeParam::print(std::ostream& out) const {
    out << "TypeParam[name=" << name_ << "]";
}

static TypeBuilder<TypeParam> type("Param");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

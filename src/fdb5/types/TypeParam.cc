/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "fdb5/types/TypesFactory.h"
#include "fdb5/types/TypeParam.h"
#include "fdb5/database/DB.h"

#include "marslib/MarsParam.h"
#include "fdb5/database/NotifyWind.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeParam::TypeParam(const std::string &name, const std::string &type) :
    Type(name, type) {
}

TypeParam::~TypeParam() {
}

void TypeParam::getValues(const MarsRequest &request,
                          const std::string &keyword,
                          eckit::StringList &values,
                          const NotifyWind &wind,
                          const DB *db) const {
    ASSERT(db);

    eckit::StringSet ax;

    db->axis(keyword, ax);

    eckit::StringList us;

    Type::getValues(request, keyword, us, wind, db);

    std::vector<Param> user;
    std::copy(us.begin(), us.end(), std::back_inserter(user));

    std::vector<Param> axis;
    std::copy(ax.begin(), ax.end(), std::back_inserter(axis));
    std::sort(axis.begin(), axis.end());

    bool windConvertion = false;
    MarsParam::substitute(request, user, axis, windConvertion);

    std::set<Param> axisSet;
    std::set<long> tables;

    for (std::vector<Param>::const_iterator i = axis.begin(); i != axis.end(); ++i) {
        axisSet.insert(*i);
        tables.insert((*i).table());
        tables.insert((*i).value() / 1000);
    }

    for (std::vector<Param>::const_iterator i = user.begin(); i != user.end(); ++i) {

        bool found = false;
        // User request in axis
        if (axisSet.find(*i) != axisSet.end()) {
            values.push_back(*i);
            continue;
        }

        long table = (*i).table();
        long value = (*i).value();

        // User has specified the table
        if (table) {

            // Try 140.xxx
            Param p(0, table * 1000 + value);
            if (axisSet.find(p) != axisSet.end()) {
                values.push_back(p);
                continue;
            }

            // Try xxxx
            Param q(0, value);
            if (axisSet.find(q) != axisSet.end()) {
                values.push_back(q);
                continue;
            }
        } else {

            // The user did not specify a table, try known tables

            for (std::set<long>::const_iterator j = tables.begin(); j != tables.end() && !found; ++j) {
                Param p(0, (*j) * 1000 + value);
                if (axisSet.find(p) != axisSet.end()) {
                    values.push_back(p);
                    found = true;;
                }
            }

        }

        if (!found) {
            values.push_back(*i);
        }
    }

    // Log::info() << "TypeParam before: " << us << std::endl;
    // Log::info() << "              after: " << values << std::endl;
    // Log::info() << "               wind: " << (windConvertion ? "true" : "false") << std::endl;
    // Log::info() << "               axis: " << ax << std::endl;

    if (windConvertion) {
        wind.notifyWind();
    }
}

bool TypeParam::match(const std::string& keyword,
                       const std::string& value1,
                       const std::string& value2) const {
    if(value1 == value2) {
        return true;
    }

    Param p1(value1);
    Param p2(value2);

    if((p1.value() == p2.value()) && (p1.table() == 0 || p2.table() == 0)) {
        return true;
    }

    if(p1.table() * 1000 + p1.value() == p2.value())
    {
        return true;
    }

    if(p2.table() * 1000 + p2.value() == p1.value())
    {
        return true;
    }

    return false;
}

void TypeParam::print(std::ostream &out) const {
    out << "TypeParam[name=" << name_ << "]";
}

static TypeBuilder<TypeParam> type("Param");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

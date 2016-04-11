/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"

#include "fdb5/Predicate.h"
#include "fdb5/Rule.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Rule::Rule(std::vector<Predicate*>& predicates, std::vector<Rule*>& rules)
{
    std::swap(predicates, predicates_);
    std::swap(rules, rules_);

//    dump(std::cout);
}

Rule::~Rule()
{
    for(std::vector<Predicate*>::iterator i = predicates_.begin(); i != predicates_.end(); ++i ) {
        delete *i;
    }

    for(std::vector<Rule*>::iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        delete *i;
    }
}

void Rule::dump(std::ostream& s, size_t depth) const
{
    s << "[";
    const char* sep = "";
    for(std::vector<Predicate*>::const_iterator i = predicates_.begin(); i != predicates_.end(); ++i ) {
        s << sep;
        (*i)->dump(s);
        sep = ",";
    }

    for(std::vector<Rule*>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->dump(s, depth+1);
    }
    s << "]";
}

void Rule::print(std::ostream& out) const
{
    out << "Rule()";
}

std::ostream& operator<<(std::ostream& s, const Rule& x)
{
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

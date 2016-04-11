/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <fstream>

#include "eckit/log/Log.h"

#include "fdb5/Rules.h"
#include "fdb5/Rule.h"
#include "fdb5/RulesParser.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Rules::Rules(const std::string& path)
{
    std::ifstream in(path.c_str());

    RulesParser parser(in);

    parser.parse(rules_);
}

Rules::~Rules()
{
    for(std::vector<Rule*>::iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        delete *i;
    }
}

void Rules::dump(std::ostream& s) const
{
    for(std::vector<Rule*>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->dump(s);
        s << std::endl;
    }
}

void Rules::print(std::ostream& out) const
{
    out << "Rules()";
}

std::ostream& operator<<(std::ostream& s, const Rules& x)
{
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

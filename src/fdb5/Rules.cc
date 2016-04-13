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

#include "marslib/MarsRequest.h"

#include "fdb5/Rules.h"
#include "fdb5/Rule.h"
#include "fdb5/Key.h"
#include "fdb5/RulesParser.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Rules::Rules()
{
}

Rules::~Rules()
{
    clear();
}

void Rules::expand(const MarsRequest& request, ReadVisitor& visitor) const
{
    Key full;
    std::vector<Key> keys(3);

    for(std::vector<Rule*>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->expand(request, visitor, 0, keys, full);
    }
}

void Rules::load(const eckit::PathName& path, bool replace)
{
    if(replace) {
        clear();
    }

    std::ifstream in(path.asString().c_str());
    if(!in) {
        throw eckit::CantOpenFile(path);
    }

    RulesParser parser(in);

    parser.parse(rules_);

    check();
}

void Rules::clear()
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

void Rules::check()
{
    for(std::vector<Rule*>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        /// @todo print offending rule in meaningful message
        ASSERT((*i)->depth() == 3);
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

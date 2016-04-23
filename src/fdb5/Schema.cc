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

#include "fdb5/Schema.h"
#include "fdb5/Rule.h"
#include "fdb5/Key.h"
#include "fdb5/SchemaParser.h"
#include "fdb5/WriteVisitor.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Schema::Schema()
{
}

Schema::~Schema()
{
    clear();
}

void Schema::expand(const MarsRequest& request, ReadVisitor& visitor) const
{
    Key full;
    std::vector<Key> keys(3);

    for(std::vector<Rule*>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->expand(request, visitor, 0, keys, full);
    }
}

void Schema::expand(const Key& field, WriteVisitor& visitor) const
{
    Key full;
    std::vector<Key> keys(3);

    visitor.rule(0); // reset to no rule so we verify that we pick at least one

    for(std::vector<Rule*>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->expand(field, visitor, 0, keys, full);
    }
}

void Schema::load(const eckit::PathName& path, bool replace)
{
    if(replace) {
        clear();
    }

    eckit::Log::info() << "Loading FDB rules from " << path << std::endl;

    std::ifstream in(path.asString().c_str());
    if(!in) {
        throw eckit::CantOpenFile(path);
    }

    SchemaParser parser(in);

    parser.parse(*this, rules_, registry_);

    check();
}

void Schema::clear()
{
    for(std::vector<Rule*>::iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        delete *i;
    }
}

void Schema::dump(std::ostream& s) const
{
    registry_.dump(s);
    for(std::vector<Rule*>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->dump(s);
        s << std::endl;
    }
}

void Schema::check()
{
    for(std::vector<Rule*>::iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        /// @todo print offending rule in meaningful message
        ASSERT((*i)->depth() == 3);
        (*i)->registry_.updateParent(&registry_);
        (*i)->updateParent(0);
    }
}

void Schema::print(std::ostream& out) const
{
    out << "Schema()";
}

const Type& Schema::lookupType(const std::string& keyword) const {
    return registry_.lookupType(keyword);
}


void Schema::compareTo(const Schema& other) const {
    // TODO
}

std::ostream& operator<<(std::ostream& s, const Schema& x)
{
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

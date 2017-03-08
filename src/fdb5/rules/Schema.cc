/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/LibFdb.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/database/Key.h"
#include "fdb5/rules/SchemaParser.h"
#include "fdb5/database/WriteVisitor.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

SchemaHasChanged::SchemaHasChanged(const Schema &schema):
    Exception("Schema has changed: " + schema.path()),
    path_(schema.path()) {
}

SchemaHasChanged::~SchemaHasChanged() throw() {

}

//----------------------------------------------------------------------------------------------------------------------

Schema::Schema() {
}

Schema::Schema(const eckit::PathName &path) {
    load(path);
}

Schema::Schema(std::istream& s) {
    load(s);
}

Schema::~Schema() {
    clear();
}

const Rule*  Schema::ruleFor(const Key& dbKey, const Key& idxKey) const {
    std::vector<Key> keys;
    keys.push_back(dbKey);
    keys.push_back(idxKey);

    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        const Rule* r = (*i)->ruleFor(keys , 0);
        if (r) {
            return r;
        }
    }
    return 0;
}

void Schema::expand(const MarsRequest &request, ReadVisitor &visitor) const {
    Key full;
    std::vector<Key> keys(3);

    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
		// eckit::Log::info() << "Rule " << **i <<  std::endl;
		// (*i)->dump(eckit::Log::info());
		(*i)->expand(request, visitor, 0, keys, full);
    }
}

void Schema::expand(const Key &field, WriteVisitor &visitor) const {
    Key full;
    std::vector<Key> keys(3);

    visitor.rule(0); // reset to no rule so we verify that we pick at least one

    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->expand(field, visitor, 0, keys, full);
    }
}

bool Schema::expandFirstLevel(const Key &dbKey,  Key &result) const {
    bool found = false;
    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end() && !found; ++i ) {
        (*i)->expandFirstLevel(dbKey, result, found);
    }
    return found;
}

void Schema::matchFirstLevel(const Key &dbKey,  std::set<Key> &result) const {
    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->matchFirstLevel(dbKey, result);
    }
}

void Schema::load(const eckit::PathName &path, bool replace) {

    path_ = path;

    eckit::Log::debug<LibFdb>() << "Loading FDB rules from " << path << std::endl;

    std::ifstream in(path.localPath());
    if (!in)
        throw eckit::CantOpenFile(path);

    load(in, replace);
}

void Schema::load(std::istream& s, bool replace) {

    if (replace) {
        clear();
    }

    SchemaParser parser(s);

    parser.parse(*this, rules_, registry_);

    check();
}

void Schema::clear() {
    for (std::vector<Rule *>::iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        delete *i;
    }
}

void Schema::dump(std::ostream &s) const {
    registry_.dump(s);
    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->dump(s);
        s << std::endl;
    }
}

void Schema::check() {
    for (std::vector<Rule *>::iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        /// @todo print offending rule in meaningful message
        ASSERT((*i)->depth() == 3);
        (*i)->registry_.updateParent(&registry_);
        (*i)->updateParent(0);
    }
}

void Schema::print(std::ostream &out) const {
    out << "Schema[path=" << path_ << "]";
}

const Type &Schema::lookupType(const std::string &keyword) const {
    return registry_.lookupType(keyword);
}



void Schema::compareTo(const Schema &other) const {
    std::ostringstream a;
    std::ostringstream b;

    dump(a);
    other.dump(b);

    if (empty()) {
        eckit::Log::warning() << *this << " is empty" << std::endl;
    }

    if (other.empty()) {
        eckit::Log::warning() << other << " is empty" << std::endl;
    }

    if (a.str() != b.str()) {
        throw SchemaHasChanged(*this);
    }
}

bool Schema::empty() const {
    return rules_.empty();
}

const std::string &Schema::path() const {
    return path_;
}

const TypesRegistry& Schema::registry() const {
    return registry_;
}


std::ostream &operator<<(std::ostream &s, const Schema &x) {
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

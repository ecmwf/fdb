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

#include "marslib/MarsRequest.h"

#include "fdb5/Predicate.h"
#include "fdb5/Rule.h"
#include "fdb5/Key.h"
#include "fdb5/Visitor.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

const Rule* matchFirst(const std::vector<Rule*>& rules, const Key& key, size_t depth)
{
    ASSERT(depth == 0);
    for(std::vector<Rule*>::const_iterator i = rules.begin(); i != rules.end(); ++i ) {
        if( (*i)->match(key) ) {
            return *i;
        }
    }
    return 0;
}

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

void Rule::expand( const MarsRequest& request,
                   std::vector<Predicate*>::const_iterator cur,
                   std::vector<Key>& keys,
                   Visitor& collector) const {

    if(cur == predicates_.end()) {
        if(rules_.empty()) {
            ASSERT(keys.size() == 3); /// we have 3 levels ATM
            collector.selectDatum( keys[2]);
        }
        else {

            switch(keys.size()) {
                case 1:
                    collector.selectDatabase(keys[0]);
                    break;

                case 2:
                    collector.selectIndex(keys[1]);
                    break;

                default:
                    ASSERT(keys.size() == 1 || keys.size() == 2);
                    break;
            }

            for(std::vector<Rule*>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
                keys.push_back(Key());
                (*i)->expand(request, collector, keys);
                keys.pop_back();
            }
        }
        return;
    }

    ++cur;

    const std::string& keyword = (*cur)->keyword();

    StringList values;

    collector.values(request, keyword, values);

    Key& k = keys.back();

    for(StringList::const_iterator i = values.begin(); i != values.end(); ++i) {

        if((*cur)->match(*i)) {

            collector.enter(keyword, *i);

            k.set(keyword, *i);
            expand(request, cur, keys, collector);
            k.unset(keyword);

            collector.leave();
        }
    }
}

void Rule::expand(const MarsRequest& request, Visitor& collector, std::vector<Key>& keys) const
{
    expand(request, predicates_.begin(), keys, collector);
}

bool Rule::match(const Key& key) const
{
    for(std::vector<Predicate*>::const_iterator i = predicates_.begin(); i != predicates_.end(); ++i ) {
        if(!(*i)->match(key)) {
            return false;
        }
    }
    return true;
}

eckit::StringList Rule::keys(size_t level) const
{
    StringList result;
    StringSet seen;
    keys(level, 0, result, seen);
    return result;
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

size_t Rule::depth() const
{
    size_t result = 0;
    for(std::vector<Rule*>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        result = std::max(result, (*i)->depth());
    }
    return result+1;
}

void Rule::keys(size_t level, size_t depth, eckit::StringList& result, eckit::StringSet& seen) const
{
    if(level>=depth) {
        for(std::vector<Predicate*>::const_iterator i = predicates_.begin(); i != predicates_.end(); ++i ) {
            const std::string& keyword = (*i)->keyword();
            if(seen.find(keyword) == seen.end()) {
                result.push_back(keyword);
                seen.insert(keyword);
            }
        }
        keys(level, depth+1, result, seen);
    }
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

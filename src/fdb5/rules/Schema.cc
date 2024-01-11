/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <fstream>

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Notifier.h"
#include "fdb5/database/RetrieveVisitor.h"
#include "fdb5/database/WriteVisitor.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/rules/SchemaParser.h"
#include "fdb5/types/Type.h"
#include "metkit/mars/MarsRequest.h"

namespace fdb5 {

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

void Schema::expand(const metkit::mars::MarsRequest &request, ReadVisitor &visitor) const {
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

void Schema::expandSecond(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, const Key& dbKey) const {

    const Rule* dbRule = nullptr;
    for (const Rule* r : rules_) {
        if (r->match(dbKey)) {
            dbRule = r;
            break;
        }
    }
    ASSERT(dbRule);

    Key full = dbKey;
    std::vector<Key> keys(3);
    keys[0] = dbKey;

    for (std::vector<Rule*>:: const_iterator i = dbRule->rules_.begin(); i != dbRule->rules_.end(); ++i) {
        (*i)->expand(request, visitor, 1, keys, full);
    }
}

void Schema::expandSecond(const Key& field, WriteVisitor& visitor, const Key& dbKey) const {

    const Rule* dbRule = nullptr;
    for (const Rule* r : rules_) {
        if (r->match(dbKey)) {
            dbRule = r;
            break;
        }
    }
    ASSERT(dbRule);

    Key full = dbKey;
    std::vector<Key> keys(3);
    keys[0] = dbKey;

    for (std::vector<Rule*>:: const_iterator i = dbRule->rules_.begin(); i != dbRule->rules_.end(); ++i) {
        (*i)->expand(field, visitor, 1, keys, full);
    }
}

bool Schema::expandFirstLevel(const Key &dbKey,  Key &result) const {
    bool found = false;
    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end() && !found; ++i ) {
        (*i)->expandFirstLevel(dbKey, result, found);
    }
    return found;
}

bool Schema::expandFirstLevel(const metkit::mars::MarsRequest& request, Key &result) const {
    std::vector<Key> results;
    bool found = expandFirstLevel(request, results);
    if (found) {
        ASSERT(found);
        result = results.front();
    }
    return found;
}

bool Schema::expandFirstLevel(const metkit::mars::MarsRequest& request, std::vector<Key>& result) const {
    bool found = false;
    for (const Rule* rule : rules_) {
        rule->expandFirstLevel(request, result, found);
        if (found) break;
    }
    return found;
}

void Schema::matchFirstLevel(const Key &dbKey,  std::set<Key> &result, const char* missing) const {
    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->matchFirstLevel(dbKey, result, missing);
    }
}

void Schema::matchFirstLevel(const metkit::mars::MarsRequest& request,  std::set<Key>& result, const char* missing) const {
    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->matchFirstLevel(request, result, missing);
    }
}

namespace {
struct LevelVisitor : public RetrieveVisitor {

    class NullNotifier : public Notifier {
        void notifyWind() const override {}
    };

    // n.b. gatherer_ initialised after passed to RetrieveVisitor. OK as it isn't actually used
    explicit LevelVisitor(const metkit::mars::MarsRequest& request, std::vector<Key>& requestBits) :
        RetrieveVisitor(notifier_, gatherer_),
        done_(false),
        gatherer_(false),
        requestBits_(requestBits) {
        auto&& params(request.params());
        keys_.insert(params.begin(), params.end());
    }

    bool selectDatabase(const Key& key, const Key& full) override {
        if (done_) return false; // Skip further exploration

        level_ = std::max(level_, 1);
        requestBits_[0] = key;

        if (full.keys() == keys_) {
            done_ = true;
            return false;
        }

        // We need to connect to the actual DB so that we can use any special case schemas
        if (RetrieveVisitor::selectDatabase(key, full)) {
            return true;
        }
        return false;
    }

    bool selectIndex(const Key& key, const Key& full) override {
        if (done_) return false; // Skip further exploration

        level_ = std::max(level_, 2);
        requestBits_[1] = key;

        if (full.keys() == keys_) {
            done_ = true;
            return false;
        }

        return true;

//        if (RetrieveVisitor::selectIndex(key, full)) {
//            return true;
//        }
//        return false;
    }

    bool selectDatum(const Key& key, const Key& full) override {
        ASSERT(full.keys() == keys_);
        done_ = true;
        level_ = 3;
        requestBits_[2] = key;
        return false;
    }

    void values(const metkit::mars::MarsRequest &request,
                const std::string &keyword,
                const TypesRegistry &registry,
                eckit::StringList &values) {

        eckit::StringList list;
        registry.lookupType(keyword).getValues(request, keyword, list, wind_, db_.get());

        for(auto l: list) {
            values.push_back(l);
        }
    }

    void print(std::ostream& out) const override {
        out << "LevelVisitor(level=" << level_ << ")";
    }

    int level() const { return level_; }

private:
    bool done_;
    HandleGatherer gatherer_; // For form only. Unused.
    NullNotifier notifier_;   // For form only. Unused.
    int level_ = 0;
    std::vector<Key>& requestBits_;
    std::set<std::string> keys_;
};
}

int Schema::fullyExpandedLevels(const metkit::mars::MarsRequest& request, std::vector<Key>& requestBits) const {

    ASSERT(requestBits.size() == 3);

    LevelVisitor visitor(request, requestBits);
    try {
        expand(request, visitor);
    } catch (const eckit::UserError&) {
        // TODO: This is clearly not very happy. The schema should not be throwing an exception if
        //       we don't match a level... we sholud just not match. This exception is being thrown
        //       in the wrong place...
        // FIXME: FIX ME
    }

    Key residual;
    for (const auto& p : request.params()) {
        const auto& vs = request.values(p);
        ASSERT(!vs.empty());
        residual.set(p, vs[0]);
    }

    for (const auto& bits : requestBits) {
        for (const auto& k : bits.keys()) {
            residual.unset(k);
        }
    }

    if (!residual.empty()) {
        ASSERT(requestBits[visitor.level()].empty());
        requestBits[visitor.level()] = residual;
    }

    return visitor.level();
}

void Schema::load(const eckit::PathName &path, bool replace) {

    path_ = path;

    eckit::Log::debug<LibFdb5>() << "Loading FDB rules from " << path << std::endl;

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

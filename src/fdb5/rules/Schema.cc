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

#include "eckit/utils/Tokenizer.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/Notifier.h"
#include "fdb5/database/RetrieveVisitor.h"
#include "fdb5/database/WriteVisitor.h"
#include "fdb5/io/HandleGatherer.h"
#include "fdb5/rules/Predicate.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/rules/SchemaParser.h"
#include "fdb5/types/Type.h"

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

    for (const RuleFirst* rule : rules_) {
        const Rule* r = rule->ruleFor(keys , 0);
        if (r) {
            return r;
        }
    }
    return nullptr;
}

void Schema::expand(const metkit::mars::MarsRequest &request, ReadVisitor &visitor) const {
    Key full;
    std::vector<Key> keys(3);

    for (const RuleFirst* rule : rules_) {
		rule->expand(request, visitor, keys, full);
    }
}

void Schema::expand(const Key &field, WriteVisitor &visitor) const {
    Key full;
    std::vector<Key> keys(3);

    visitor.rule(nullptr); // reset to no rule so we verify that we pick at least one

    for (const RuleFirst* rule : rules_) {
        rule->expand(field, visitor, keys, full);
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
        (*i)->expand(request, visitor, keys, full);
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
        (*i)->expand(field, visitor, keys, full);
    }
}

bool Schema::expandFirstLevel(const Key &dbKey,  Key &result) const {
    bool found = false;
    for (const RuleFirst* rule : rules_) {
        rule->expandFirstLevel(dbKey, result, found);
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
    for (const RuleFirst* rule : rules_) {
        rule->expandFirstLevel(request, result, found);
        if (found) break;
    }
    return found;
}

void Schema::matchFirstLevel(const Key &dbKey,  std::set<Key> &result, const char* missing) const {
    for (const RuleFirst* rule : rules_) {
        rule->matchFirstLevel(dbKey, result, missing);
    }
}

void Schema::matchFirstLevel(const metkit::mars::MarsRequest& request,  std::set<Key>& result, const char* missing) const {
    for (const RuleFirst* rule : rules_) {
        rule->matchFirstLevel(request, result, missing);
    }
}

bool Schema::matchFirstLevel(const std::string& fingerprint, Key& key) const {

    eckit::Tokenizer parse(":", true);
    eckit::StringList values;
    parse(fingerprint, values);

    bool found = false;
    for (const RuleFirst* rule : rules_) {
        Key filledKey;
        if (rule->tryFill(filledKey, values)) {
            rule->expandFirstLevel(filledKey, key, found);
            if (found) {
                ASSERT(filledKey == key);
                return true;
            }
        }
    }

    return false;
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

    void storeSubRules(const Rule& rule) {
        keySeq_.clear();
        for (const auto& r : rule.subRules()) {
            keySeq_.emplace_back();
            for (const auto& predicate : r->predicates()) {
                keySeq_.back().push_back(predicate->keyword());
            }
        }
    }

    bool selectDatabase(const Key& key, const Key& full) override {
        if (done_) return false; // Skip further exploration

        level_ = std::max(level_, 1);
        requestBits_[0] = key;

        storeSubRules(*key.rule());

        if (full.keys() == keys_) {
            done_ = true;
            return false;
        }

        // We need to connect to the actual DB so that we can use any special case schemas
        if (RetrieveVisitor::selectDatabase(key, full)) {
            return true;
        } else {
            // FIXME: If DB not found, not invalid for testing this. We just need to
            //        use the primary schema
        }
        return false;
    }

    bool selectIndex(const Key& key, const Key& full) override {
        if (done_) return false; // Skip further exploration

        level_ = std::max(level_, 2);
        requestBits_[1] = key;

        storeSubRules(*key.rule());

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

    const std::vector<std::vector<std::string>>& keySeq() const { return keySeq_; }

private:
    bool done_;
    HandleGatherer gatherer_; // For form only. Unused.
    NullNotifier notifier_;   // For form only. Unused.
    int level_ = 0;
    std::vector<Key>& requestBits_;
    std::set<std::string> keys_;
    std::vector<std::vector<std::string>> keySeq_;
};
}

int Schema::fullyExpandedLevels(const metkit::mars::MarsRequest& request,
                                std::vector<Key>& requestBits,
                                std::vector<std::string>* nextKeys) const {

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

    // Examine the key sequences for the next rules

    if (nextKeys) {
        for (const auto& seq : visitor.keySeq()) {
            std::set<std::string> keys(requestBits[visitor.level()].keys());
            for (const auto& k : seq) {
                if (keys.empty()) {
                    nextKeys->push_back(k);
                    break;
                }
                auto it = keys.find(k);
                if (it == keys.end()) {
                    break;
                } else {
                    keys.erase(it);
                }
            }
        }
    }

    return visitor.level();
}

void Schema::load(const eckit::PathName &path, bool replace) {

    path_ = path;

    LOG_DEBUG_LIB(LibFdb5) << "Loading FDB rules from " << path << std::endl;

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
    for (const RuleFirst* rule : rules_) {
        delete rule;
    }
    rules_.clear();
}

void Schema::dump(std::ostream &s) const {
    registry_.dump(s);
    for (const RuleFirst* rule : rules_) {
        rule->dump(s);
        s << std::endl;
    }
}

void Schema::check() {
    for (RuleFirst* rule : rules_) {
        /// @todo print offending rule in meaningful message
        ASSERT(rule->depth() == 3);
        rule->registry_.updateParent(&registry_);
        rule->updateParent(0);
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

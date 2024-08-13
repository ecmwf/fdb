/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/rules/Rule.h"

#include <algorithm>

#include "eckit/os/BackTrace.h"
#include "eckit/config/Resource.h"

#include "metkit/mars/MarsRequest.h"

#include "fdb5/rules/Predicate.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/database/ReadVisitor.h"
#include "fdb5/database/WriteVisitor.h"
#include "fdb5/types/Type.h"
#include "fdb5/database/CartesianProduct.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Rule::Rule(const Schema &schema,
           size_t line,
           std::vector<Predicate *>&& predicates,
           std::vector<Rule *>&& rules,
           const std::map<std::string, std::string>& types,
           long level):
    schema_(schema),
    parent_(nullptr),
    level_(level),
    predicates_(std::move(predicates)),
    rules_(std::move(rules)),
    line_(line) {
    for (std::map<std::string, std::string>::const_iterator i = types.begin(); i != types.end(); ++i) {
        registry_.addType(i->first, i->second);
    }
}

Rule::~Rule() {
//    eckit::Log::info() << " === Deleting rule " << this << std::endl;
//    eckit::Log::info() << eckit::BackTrace::dump() << std::endl;
    for (std::vector<Predicate *>::iterator i = predicates_.begin(); i != predicates_.end(); ++i ) {
        delete *i;
    }

    for (std::vector<Rule *>::iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        delete *i;
    }
}

Rule* Rule::makeRule(int level,
                     const Schema& schema,
                     size_t line,
                     std::vector<Predicate *>&& predicates,
                     std::vector<Rule *>&& rules,
                     const std::map<std::string, std::string>& types) {

    switch (level) {
    case 0:
        return new RuleFirst(schema, line, std::move(predicates), std::move(rules), types);
    case 1:
        return new RuleSecond(schema, line, std::move(predicates), std::move(rules), types);
    case 2:
        return new RuleThird(schema, line, std::move(predicates), std::move(rules), types);
    default:
        ASSERT(false);
    }

    return nullptr; // Avoid (unnecessary) compiler warning
}

#if 0
void Rule::expand( const metkit::mars::MarsRequest &request,
                   std::vector<Predicate *>::const_iterator cur,
                   size_t depth,
                   std::vector<Key> &keys,
                   Key &full,
                   ReadVisitor &visitor) const {

	ASSERT(depth < 3);

//    eckit::Log::info() << "depth: " << depth << " - predicates: " << predicates_.size() << " cur " << (cur - predicates_.begin()) << std::endl;

    if (cur == predicates_.end()) {

        // TODO: join these 2 methods
        keys[depth].rule(this);

//        eckit::Log::info() << "RQ Setting rule, level: " << depth << " : " << this << std::endl;

        if (rules_.empty()) {
            ASSERT(depth == 2); /// we have 3 levels ATM
            if (!visitor.selectDatum( keys[2], full)) {
                return; // This it not useful
            }
        } else {

            switch (depth) {
            case 0:
                if (!visitor.selectDatabase(keys[0], full)) {
                    return;
                };

                // Here we recurse on the database's schema (rather than the master schema)
                ASSERT(keys[0] == full);
                visitor.databaseSchema().expandSecond(request, visitor, keys[0]);
                return;

            case 1:
                if (!visitor.selectIndex(keys[1], full)) {
                    return;
                }
                break;

            default:
                ASSERT(depth == 0 || depth == 1);
                break;
            }

            for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
                (*i)->expand(request, visitor, depth + 1, keys, full);
            }
        }
        return;
    }

    std::vector<Predicate *>::const_iterator next = cur;
    ++next;

    const std::string &keyword = (*cur)->keyword();

    eckit::StringList values;
    visitor.values(request, keyword, registry_, values);

    // eckit::Log::info() << "keyword " << keyword << " values " << values << std::endl;

    Key &k = keys[depth];

    if (values.empty() && (*cur)->optional()) {
        values.push_back((*cur)->defaultValue());
    }

    for (eckit::StringList::const_iterator i = values.begin(); i != values.end(); ++i) {

        k.push(keyword, *i);
        full.push(keyword, *i);

        if ((*cur)->match(k))
            expand(request, next, depth, keys, full, visitor);

        full.pop(keyword);
        k.pop(keyword);

    }

}

void Rule::expand(const metkit::mars::MarsRequest &request, ReadVisitor &visitor, size_t depth, std::vector<Key> &keys, Key &full) const {
    ASSERT(keys.size() == 3);
    expand(request, predicates_.begin(), depth, keys, full, visitor);
}
#endif

void Rule::expand(const metkit::mars::MarsRequest& request,
                  ReadVisitor& visitor,
                  std::vector<Key>& keys,
                  Key& full) const {

    // TODO: Stop walking the schema once we have found the matching rule...
    // TODO: Canonicalise the MarsRequest in here, rather than further down in the expression --> CanonicalMarsRequest???

    ASSERT(keys.size() == 3);

    // Test if the predicate is present in the key (if not optional).
    // If not supplied, skip all further processing and iteration
    for (const Predicate* predicate : predicates_) {
        const std::string& keyword(predicate->keyword());
        if (!request.has(keyword) || request.countValues(keyword) == 0) {
            if (!predicate->optional()) return;
        }
    }

    Key& ruleKey(keys[level_]);
    ruleKey.clear();
    CartesianProduct<std::vector<std::string>> product;

    // We know that this key matches the structure of the Rule. Now we
    // parse relevant keys according to the Type system, and filter them
    // according to any relevant axis information
    //
    // n.b. don't combine with the above, to avoid constructing Axes unnecessarily
    std::vector<eckit::StringList> valueStore;
    valueStore.reserve(predicates_.size());
    for (const Predicate* predicate : predicates_) {
        std::string keyword(predicate->keyword());

        eckit::StringList values;
        // TODO: We can do this better than this... We don't need to do it for every rule. Again and again...
        visitor.values(request, keyword, registry_, values);

        if (values.empty()) {
            ASSERT(predicate->optional());
            values.push_back(predicate->defaultValue());
        } else {
            auto new_end = std::remove_if(values.begin(),
                                          values.end(),
                                          [&predicate](const std::string& v) { return !predicate->match(v); });
            values.erase(new_end, values.end());
            if (values.empty()) return;
        }

        valueStore.emplace_back(std::move(values));
        ruleKey.push(keyword, "");
        product.append(valueStore.back(), ruleKey.mutableValue(keyword));
    }

    // Iterate through the permutations possible from the available keys

    // TODO: short circuit this stuff once everything has been matched...
    // TODO: Ensure that the key is fully consumed
    ruleKey.rule(this);
    while(product.next()) {
        for (const auto& kv : ruleKey) full.set(kv.first, kv.second);
        walkNextLevel(request, visitor, keys, full);
    }
}

// Expand the first level when writing (then delegate to next level)

void Rule::expand(const Key &field,
                  WriteVisitor& visitor,
                  std::vector<Key>& keys,
                  Key& full) const {

    // This ensures we skip all rules after the first matching one.
    // TODO: implement this by changing the return type of expand
    // TODO: Canonicalise the key in this function, rather than further down in the expression --> CanonicalKey???
    if (visitor.rule()) return;

    ASSERT(keys.size() == 3);

    // Test if the predicate is present in the key (if not optional).
    // If not supplied, skip all further processing and iteration
    for (const Predicate* predicate : predicates_) {
        const std::string& keyword(predicate->keyword());
        if (!field.has(keyword)) {
            if (!predicate->optional()) return;
        }
    }

    // We know that this key matches the structure of the Rule. Now we
    // parse relevant keys according to the Type system, and filter them
    // according to any relevant axis information
    //
    // n.b. don't combine with the above, to avoid constructing Axes unnecessarily
    Key ruleKey;
    for (const Predicate* predicate : predicates_) {
        const std::string& keyword(predicate->keyword());
        const std::string& value(predicate->value(field));
        if (!predicate->match(value)) return;
        ruleKey.push(keyword, value);
    }

    // TODO: visitor.filter(ruleKey);. Do this according to the current DB

    // Now we walk through the predicates recursively and construct the
    // relevant keys
    // n.b. nothing to do if archiving - as only one KEY

    // Before calling the next level in the rules
    // TODO: Ensure that the key is fully consumed
    for (const auto& kv : ruleKey) full.push(kv.first, kv.second);
    keys[level_] = std::move(ruleKey);
    keys[level_].rule(this);
    walkNextLevel(field, visitor, keys, full);
}

#if 0
void Rule::expand( const Key &field,
                   std::vector<Predicate *>::const_iterator cur,
                   size_t depth,
                   std::vector<Key> &keys,
                   Key &full,
                   WriteVisitor &visitor) const {

    static bool matchFirstFdbRule = eckit::Resource<bool>("matchFirstFdbRule", true);

    if (matchFirstFdbRule && visitor.rule()) {
        return;
    }

    ASSERT(depth < 3);


    if (cur == predicates_.end()) {

        keys[depth].rule(this);

        if (rules_.empty()) {
            ASSERT(depth == 2); /// we have 3 levels ATM
            if (visitor.rule() != 0) {
                std::ostringstream oss;
                oss << "More than one rule matching "
                    << keys[0] << ", "
                    << keys[1] << ", "
                    << keys[2] << " "
                    << topRule() << " and "
                    << visitor.rule()->topRule();
                throw eckit::SeriousBug(oss.str());
            }
            visitor.rule(this);
            visitor.selectDatum( keys[2], full);
        } else {

            switch (depth) {
            case 0:
                if (keys[0] != visitor.prev_[0] /*|| keys[0].registry() != visitor.prev_[0].registry()*/) {
                    visitor.selectDatabase(keys[0], full);
                    visitor.prev_[0] = keys[0];
                    visitor.prev_[1] = Key();
                }

                // Here we recurse on the database's schema (rather than the master schema)
                visitor.databaseSchema().expandSecond(field, visitor, keys[0]);
                return;

            case 1:
                if (keys[1] != visitor.prev_[1] /*|| keys[1].registry() != visitor.prev_[1].registry()*/) {
                    visitor.selectIndex(keys[1], full);
                    visitor.prev_[1] = keys[1];
                }
                break;

            default:
                ASSERT(depth == 0 || depth == 1);
                break;
            }

            for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
                (*i)->expand(field, visitor, depth + 1, keys, full);
            }
        }
        return;
    }

    std::vector<Predicate *>::const_iterator next = cur;
    ++next;

    const std::string &keyword = (*cur)->keyword();
    const std::string &value = (*cur)->value(field);

    Key &k = keys[depth];

    k.push(keyword, value);
    full.push(keyword, value);

    if ((*cur)->match(k)) {
        expand(field, next, depth, keys, full, visitor);
    }

    full.pop(keyword);
    k.pop(keyword);
}
#endif


void RuleFirst::expandFirstLevel( const Key &dbKey, std::vector<Predicate *>::const_iterator cur, Key &result, bool& found) const {

    if (cur == predicates_.end()) {
        found = true;
        result.rule(this);
        return;
    }

    std::vector<Predicate *>::const_iterator next = cur;
    ++next;

    const std::string &keyword = (*cur)->keyword();
    const std::string &value = (*cur)->value(dbKey);

    result.push(keyword, value);

    if ((*cur)->match(result)) {
        expandFirstLevel(dbKey, next, result, found);
    }

    if (!found) {
        result.pop(keyword);
    }
}

void RuleFirst::expandFirstLevel(const Key &dbKey,  Key& result, bool& found) const {
    expandFirstLevel(dbKey, predicates_.begin(), result, found);
}

void RuleFirst::expandFirstLevel(const metkit::mars::MarsRequest& rq,
                            std::vector<Predicate *>::const_iterator cur,
                            std::vector<Key>& result,
                            Key& working,
                            bool& found) const {

    if (cur == predicates_.end()) {
        found = true;
        working.rule(this);
        result.push_back(working);
        return;
    }

    std::vector<Predicate *>::const_iterator next = cur;
    ++next;

    const std::string& keyword = (*cur)->keyword();
    const std::vector<std::string>& values = (*cur)->values(rq);

    for (const std::string& value : values) {

        working.push(keyword, value);

        if ((*cur)->match(working)) {
            expandFirstLevel(rq, next, result, working, found);
        }

        working.pop(keyword);
    }
}

void RuleFirst::expandFirstLevel(const metkit::mars::MarsRequest& request, std::vector<Key>& result, bool& done) const {
    Key working;
    expandFirstLevel(request, predicates_.begin(), result, working, done);
}


void RuleFirst::matchFirstLevel( const Key &dbKey, std::vector<Predicate *>::const_iterator cur, Key& tmp, std::set<Key>& result, const char* missing) const {

    if (cur == predicates_.end()) {
        if (tmp.match(dbKey)) {
            result.insert(tmp);
        }
        return;
    }

    std::vector<Predicate *>::const_iterator next = cur;
    ++next;

    const std::string &keyword = (*cur)->keyword();

    if (dbKey.find(keyword) == dbKey.end()) {
        tmp.push(keyword, missing);
        matchFirstLevel(dbKey, next, tmp, result, missing);
    } else {
        const std::string &value = (*cur)->value(dbKey);

        tmp.push(keyword, value);

        if ((*cur)->match(tmp)) {
            matchFirstLevel(dbKey, next, tmp, result, missing);
        }
    }

    tmp.pop(keyword);

}

void RuleFirst::matchFirstLevel(const Key &dbKey,  std::set<Key>& result, const char* missing) const {
    Key tmp;
    matchFirstLevel(dbKey, predicates_.begin(), tmp, result, missing);
}


void RuleFirst::matchFirstLevel(const metkit::mars::MarsRequest& request, std::vector<Predicate *>::const_iterator cur, Key& tmp, std::set<Key>& result, const char* missing) const {

    if (cur == predicates_.end()) {
//        if (tmp.match(request)) {
            result.insert(tmp);
//        }
        return;
    }

    std::vector<Predicate *>::const_iterator next = cur;
    ++next;

    const std::string& keyword = (*cur)->keyword();

    if (request.has(keyword)) {

        const std::vector<std::string>& values = (*cur)->values(request);

        for (const std::string& value : values) {
            tmp.push(keyword, value);
            if ((*cur)->match(tmp)) {
                matchFirstLevel(request, next, tmp, result, missing);
            }
            tmp.pop(keyword);
        }
    } else {
        tmp.push(keyword, missing);
        matchFirstLevel(request, next, tmp, result, missing);
        tmp.pop(keyword);
    }
}

void RuleFirst::matchFirstLevel(const metkit::mars::MarsRequest& request,  std::set<Key>& result, const char* missing) const {
    Key tmp;
    matchFirstLevel(request, predicates_.begin(), tmp, result, missing);
}


bool Rule::match(const Key &key) const {
    for (std::vector<Predicate *>::const_iterator i = predicates_.begin(); i != predicates_.end(); ++i ) {
        if (!(*i)->match(key)) {
            return false;
        }
    }
    return true;
}

// Find the first rule that matches a list of keys
const Rule* Rule::ruleFor(const std::vector<fdb5::Key> &keys, size_t depth) const {

    if (depth == keys.size()) {
        return this;
    }

    if (match(keys[depth])) {

        for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
            const Rule *r = (*i)->ruleFor(keys, depth + 1);
            if (r) {
                return r;
            }
        }
    }
    return 0;
}

bool Rule::tryFill(Key& key, const eckit::StringList& values) const {

    // See FDB-103. This is a hack to work around the indexing abstraction
    // being leaky.
    //
    // i) Indexing is according to a colon-separated string of values
    // ii) This string of values is passed to, and split in, the constructor
    //     of Key().
    // iii) The constructor of Key does not know what these values correspond
    //      to, so calls this function to map them to the Predicates.
    //
    // This whole process really ought to take place inside of (Toc)-Index,
    // such that a Key() is returned, and any kludgery is contained there.
    // But that is too large a change to safely make this close to
    // operational switchover day.
    //
    // --> HACK.
    // --> Stick a plaster over the symptom.


    auto it_value = values.begin();
    auto it_pred = predicates_.begin();

    for (; it_pred != predicates_.end() && it_value != values.end(); ++it_pred, ++it_value) {

        if (values.size() == (predicates_.size() + 1) && (*it_pred)->keyword() == "quantile") {
            std::string actualQuantile = *it_value;
            ++it_value;
            if (it_value == values.end()) return false;
            actualQuantile += std::string(":") + (*it_value);
            (*it_pred)->fill(key, actualQuantile);
        } else {
            (*it_pred)->fill(key, *it_value);
        }
    }

    // Check that everything is exactly consumed
    if (it_value != values.end()) return false;
    if (it_pred != predicates_.end()) return false;
    return true;
}

void Rule::fill(Key& key, const eckit::StringList& values) const {

    // FDB-103 - see comment in fill re quantile

    ASSERT(values.size() >= predicates_.size()); // Should be equal, except for quantile (FDB-103)
    ASSERT(values.size() <= predicates_.size() + 1);
    ASSERT(tryFill(key, values));
}

void Rule::dump(std::ostream &s, size_t depth) const {
    s << "[";
    const char *sep = "";
    for (std::vector<Predicate *>::const_iterator i = predicates_.begin(); i != predicates_.end(); ++i ) {
        s << sep;
        (*i)->dump(s, registry_);
        sep = ",";
    }

    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->dump(s, depth + 1);
    }
    s << "]";
}

size_t Rule::depth() const {
    size_t result = 0;
    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        result = std::max(result, (*i)->depth());
    }
    return result + 1;
}

void Rule::updateParent(const Rule *parent) {
    parent_ = parent;
    if (parent) {
        registry_.updateParent(&parent_->registry_);
    }
    for (std::vector<Rule *>::iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->updateParent(this);
    }
}

const TypesRegistry &Rule::registry() const {
    return registry_;
}

void Rule::print(std::ostream &out) const {
    out << "Rule[line=" << line_ ;
    out << "]";
}

const Rule &Rule::topRule() const {
    if (parent_) {
        return parent_->topRule();
    } else {
        return *this;
    }
}

const Schema &Rule::schema() const {
    return schema_;
}

void Rule::check(const Key& key) const {
    for (const auto& pred : predicates_ ) {
        auto k = key.find(pred->keyword());
        if (k != key.end()) {
            const std::string& value = (*k).second;
            const Type& type = registry_.lookupType(pred->keyword());
            if (value != type.tidy(pred->keyword(), value)) {
                std::stringstream ss;
                ss << "Rule check - metadata not valid (not in canonical form) - found: ";
                ss << pred->keyword() << "=" << value << " - expecting " << type.tidy(pred->keyword(), value) << std::endl;
                throw eckit::UserError(ss.str(), Here());
            }
        }
    }
    if (parent_ != nullptr) {
        parent_->check(key);
    }
}

const std::vector<Predicate*>& Rule::predicates() const {
    return predicates_;
}

const std::vector<Rule*>& Rule::subRules() const {
    return rules_;
}

std::ostream &operator<<(std::ostream &s, const Rule &x) {
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

RuleThird::RuleThird(const Schema& schema,
                     size_t line,
                     std::vector<Predicate *>&& predicates,
                     std::vector<Rule *>&& rules,
                     const std::map<std::string, std::string>& types) :
    Rule(schema, line, std::move(predicates), std::move(rules), types, 2) {}

void RuleThird::walkNextLevel(const Key& field,
                               WriteVisitor& visitor,
                               std::vector<fdb5::Key>& keys,
                               Key& full) const {
    if (visitor.rule()) {
        std::ostringstream oss;
        oss << "More than one rule matching "
            << keys[0] << ", "
            << keys[1] << ", "
            << keys[2] << " "
            << topRule() << " and "
            << visitor.rule()->topRule();
        throw eckit::SeriousBug(oss.str());
    }

    visitor.rule(this);
    visitor.selectDatum(keys[2], full);
}

void RuleThird::walkNextLevel(const metkit::mars::MarsRequest& request,
                               ReadVisitor& visitor,
                               std::vector<fdb5::Key>& keys,
                               Key& full) const {
    visitor.selectDatum(keys[2], full);
}

//----------------------------------------------------------------------------------------------------------------------

RuleSecond::RuleSecond(const Schema& schema,
           size_t line,
           std::vector<Predicate *>&& predicates,
           std::vector<Rule *>&& rules,
           const std::map<std::string, std::string>& types) :
    Rule(schema, line, std::move(predicates), std::move(rules), types, 1) {}

void RuleSecond::walkNextLevel(const Key& field,
                              WriteVisitor& visitor,
                              std::vector<fdb5::Key>& keys,
                              Key& full) const {

    if (keys[1] != visitor.prev_[1]) {
        visitor.selectIndex(keys[1], full);
        visitor.prev_[1] = keys[1];
    }

    for (const Rule* rule : rules_) {
        rule->expand(field, visitor, keys, full);
    }
}

void RuleSecond::walkNextLevel(const metkit::mars::MarsRequest& request,
                               ReadVisitor& visitor,
                               std::vector<fdb5::Key>& keys,
                               Key& full) const {

    if (!visitor.selectIndex(keys[1], full)) return;

    for (const Rule* rule : rules_) {
        rule->expand(request, visitor, keys, full);
    }
}

//----------------------------------------------------------------------------------------------------------------------

RuleFirst::RuleFirst(const Schema& schema,
                     size_t line,
                     std::vector<Predicate *>&& predicates,
                     std::vector<Rule *>&& rules,
                     const std::map<std::string, std::string>& types) :
    Rule(schema, line, std::move(predicates), std::move(rules), types, 0) {}

void RuleFirst::walkNextLevel(const Key& field,
                              WriteVisitor& visitor,
                              std::vector<fdb5::Key>& keys,
                              Key& full) const {

    if (keys[0] != visitor.prev_[0]) {
        visitor.selectDatabase(keys[0], full);
        visitor.prev_[0] = keys[0];
        visitor.prev_.clear();
    }

    // Here we recurse on the database's schema (rather than the master schema)
    visitor.databaseSchema().expandSecond(field, visitor, keys[0]);
}

void RuleFirst::walkNextLevel(const metkit::mars::MarsRequest& request,
                              ReadVisitor& visitor,
                              std::vector<fdb5::Key>& keys,
                              Key& full) const {

    ASSERT(keys[0] == full);
    if (!visitor.selectDatabase(keys[0], full)) return;

    // Here we recurse on the database's schema (rather than the master schema)
    visitor.databaseSchema().expandSecond(request, visitor, keys[0]);
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

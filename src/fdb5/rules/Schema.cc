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
#include "fdb5/rules/Schema.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/database/Key.h"
#include "fdb5/rules/SchemaParser.h"
#include "fdb5/database/WriteVisitor.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

eckit::ClassSpec Schema::classSpec_ = { &eckit::Streamable::classSpec(), "Schema", };

eckit::Reanimator<Schema> Schema::reanimator_;

Schema::Schema() : registry_(new TypesRegistry()) {

}

Schema::Schema(const eckit::PathName &path) : registry_(new TypesRegistry()) {
    load(path);
}

Schema::Schema(std::istream& s) : registry_(new TypesRegistry()) {
    load(s);
}
Schema::Schema(eckit::Stream& s) :
    registry_(new TypesRegistry(s)) {

    size_t numRules;
    s >> path_;
    s >> numRules;
    for (size_t i=0; i < numRules; i++) {
        rules_.push_back(new Rule(*this, s));
    }

    check();
}

void Schema::encode(eckit::Stream& s) const {
    registry_->encode(s);
    s << path_;
    s << rules_.size();
    for (const Rule* rule : rules_) {
        rule->encode(s);
    }
}

Schema::~Schema() {
    clear();
}

const Rule*  Schema::ruleFor(const Key& dbKey, const Key& idxKey) const {
    std::vector<Key> keys;
    keys.push_back(dbKey);
    keys.push_back(idxKey);

    for (const Rule* rule : rules_) {
        const Rule* r = rule->ruleFor(keys , 0);
        if (r) {
            return r;
        }
    }
    return 0;
}

void Schema::expand(const metkit::mars::MarsRequest &request, ReadVisitor &visitor) const {
    TypedKey fullComputedKey{registry()};
    std::vector<TypedKey> keys(3, TypedKey{{}, registry()});

    for (Rule* r : rules_) {
		// eckit::Log::info() << "Rule " << **i <<  std::endl;
		// (*i)->dump(eckit::Log::info());
		r->expand(request, visitor, 0, keys, fullComputedKey);
    }
}

void Schema::expand(const Key& field, WriteVisitor &visitor) const {
    TypedKey fullComputedKey{registry()};
    std::vector<TypedKey> keys(3, TypedKey{{}, registry()});

    visitor.rule(0); // reset to no rule so we verify that we pick at least one

    for (Rule* r : rules_) {
        r->expand(field, visitor, 0, keys, fullComputedKey);
    }
}

void Schema::expandSecond(const metkit::mars::MarsRequest& request, ReadVisitor& visitor, const Key& dbKey) const {

    const Rule* dbRule = nullptr;
    for (const Rule* rule : rules_) {
        if (rule->match(dbKey)) {
            dbRule = rule;
            break;
        }
    }
    ASSERT(dbRule);

    std::vector<TypedKey> keys(3, TypedKey{{}, registry()});
    TypedKey fullComputedKey = keys[0] = TypedKey{dbKey, registry()};

    for (std::vector<Rule*>:: const_iterator i = dbRule->rules_.begin(); i != dbRule->rules_.end(); ++i) {
        (*i)->expand(request, visitor, 1, keys, fullComputedKey);
    }
}

void Schema::expandSecond(const Key& field, WriteVisitor& visitor, const Key& dbKey) const {

    const Rule* dbRule = nullptr;
    for (const Rule* rule : rules_) {
        if (rule->match(dbKey)) {
            dbRule = rule;
            break;
        }
    }
    ASSERT(dbRule);

    std::vector<TypedKey> keys(3, TypedKey{{}, registry()});
    TypedKey fullComputedKey = keys[0] = TypedKey{dbKey, registry()};

    for (std::vector<Rule*>:: const_iterator i = dbRule->rules_.begin(); i != dbRule->rules_.end(); ++i) {
        (*i)->expand(field, visitor, 1, keys, fullComputedKey);
    }
}

bool Schema::expandFirstLevel(const metkit::mars::MarsRequest& request, TypedKey& result) const {
    bool found = false;
    for (const Rule* rule : rules_) {
        rule->expandFirstLevel(request, result, found);
        if (found) {
            result.registry(rule->registry());
            break;
        }
    }
    return found;
}

void Schema::matchFirstLevel(const Key& dbKey,  std::set<Key> &result, const char* missing) const {
    for (const Rule* rule : rules_) {
        rule->matchFirstLevel(dbKey, result, missing);
    }
}

void Schema::matchFirstLevel(const metkit::mars::MarsRequest& request,  std::set<Key>& result, const char* missing) const {
    for (const Rule* rule : rules_) {
        rule->matchFirstLevel(request, result, missing);
    }
}

void Schema::load(const eckit::PathName &path, bool replace) {

    path_ = path;

    LOG_DEBUG_LIB(LibFdb5) << "Loading FDB rules from " << path << std::endl;

    std::ifstream in(path.localPath());
    if (!in) {
        auto ex = eckit::CantOpenFile(path);
        ex.dumpStackTrace();
        throw ex;
    }
    load(in, replace);
}

void Schema::load(std::istream& s, bool replace) {

    if (replace) {
        clear();
    }

    SchemaParser parser(s);

    parser.parse(*this, rules_, *registry_);

    check();
}

void Schema::clear() {
    for (std::vector<Rule *>::iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        delete *i;
    }
}

void Schema::dump(std::ostream &s) const {
    registry_->dump(s);
    for (std::vector<Rule *>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->dump(s);
        s << std::endl;
    }
}

void Schema::check() {
    for (Rule* rule : rules_) {
        /// @todo print offending rule in meaningful message
        ASSERT(rule->depth() == 3);
        rule->registry_->updateParent(registry_);
        rule->updateParent(0);
    }
}

void Schema::print(std::ostream &out) const {
    out << "Schema[path=" << path_ << "]";
}

const Type &Schema::lookupType(const std::string &keyword) const {
    return registry_->lookupType(keyword);
}


bool Schema::empty() const {
    return rules_.empty();
}

const std::string &Schema::path() const {
    return path_;
}

const std::shared_ptr<TypesRegistry> Schema::registry() const {
    return registry_;
}


std::ostream &operator<<(std::ostream &s, const Schema &x) {
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

SchemaRegistry& SchemaRegistry::instance() {
    static SchemaRegistry me;
    return me;
}

const Schema& SchemaRegistry::add(const eckit::PathName& path, Schema* schema) {
    ASSERT(schema);
    schemas_[path] = std::unique_ptr<Schema>(schema);
    return *schemas_[path];
}

const Schema& SchemaRegistry::get(const eckit::PathName& path) {
    std::lock_guard<std::mutex> lock(m_);
    auto it = schemas_.find(path);
    if (it != schemas_.end()) {
        return *it->second;
    }

    Schema* p = new Schema(path);
    ASSERT(p);
    schemas_[path] = std::unique_ptr<Schema>(p);
    return *schemas_[path];
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   SchemaParser.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#include "fdb5/rules/SchemaParser.h"
#include "fdb5/rules/ExcludeAll.h"
#include "fdb5/rules/MatchAlways.h"
#include "fdb5/rules/MatchAny.h"
#include "fdb5/rules/MatchHidden.h"
#include "fdb5/rules/MatchOptional.h"
#include "fdb5/rules/MatchValue.h"
#include "fdb5/rules/Predicate.h"
#include "fdb5/rules/Rule.h"

#include <memory>
#include <utility>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

std::string SchemaParser::parseIdent(bool value, bool emptyOK) {
    std::string s;
    for (;;) {
        char c = peek();
        switch (c) {
            case 0:
            case '/':
            case '=':
            case ',':
            case ';':
            case ':':
            case '[':
            case ']':
            case '?':
                if (s.empty() && !emptyOK) {
                    throw StreamParser::Error("Syntax error: found '" + std::to_string(c) + "'", line_ + 1);
                }
                return s;
            case '-':
                if (s.empty() && !emptyOK) {
                    throw StreamParser::Error("Syntax error: found '-'", line_ + 1);
                }
                if (!value) {
                    return s;
                }
                [[fallthrough]];
            default:
                consume(c);
                s += c;
                break;
        }
    }
}

std::unique_ptr<Predicate> SchemaParser::parsePredicate(eckit::StringDict& types) {

    bool exclude = false;
    std::set<std::string> values;
    std::string k = parseIdent(false, false);

    char c = peek();

    if (c == ':') {
        consume(c);
        ASSERT(types.find(k) == types.end());
        types[k] = parseIdent(false, false);
        c = peek();
    }

    if (c == '?') {
        consume(c);
        return std::make_unique<Predicate>(k, new MatchOptional(parseIdent(true, true)));
    }

    if (c == '-') {
        consume(c);
        if (types.find(k) == types.end()) {
            // Register ignore type
            types[k] = "Ignore";
        }
        return std::make_unique<Predicate>(k, new MatchHidden(parseIdent(true, true)));
    }

    if (c != ',' && c != '[' && c != ']') {
        consume("=");

        std::string val = parseIdent(true, false);
        exclude = val[0] == '!';

        if (exclude) {
            values.insert(val.substr(1));
        }
        else {
            values.insert(val);
        }

        while ((c = peek()) == '/') {
            consume(c);
            values.insert(parseIdent(true, false));
        }
    }

    switch (values.size()) {
        case 0:
            return std::make_unique<Predicate>(k, new MatchAlways());
        case 1:
            return std::make_unique<Predicate>(k, new MatchValue(*values.begin()));
        default:
            if (exclude) {
                return std::make_unique<Predicate>(k, new ExcludeAll(values));
            }
            return std::make_unique<Predicate>(k, new MatchAny(values));
    }
}

void SchemaParser::parseTypes(eckit::StringDict& types) {
    for (;;) {
        const auto name = parseIdent(false, true);
        if (name.empty()) {
            break;
        }
        consume(':');
        const auto type = parseIdent(false, false);
        consume(';');
        ASSERT(types.find(name) == types.end());
        types[name] = type;
    }
}

std::unique_ptr<RuleDatum> SchemaParser::parseDatum() {
    Rule::Predicates predicates;
    eckit::StringDict types;

    consume('[');

    const std::size_t line = line_ + 1;

    char c = peek();
    if (c == ']') {
        consume(c);
        return std::make_unique<RuleDatum>(line, predicates, types);
    }

    for (;;) {

        c = peek();

        predicates.emplace_back(parsePredicate(types));
        while ((c = peek()) == ',') {
            consume(c);
            predicates.emplace_back(parsePredicate(types));
        }

        c = peek();
        if (c == ']') {
            consume(c);
            return std::make_unique<RuleDatum>(line, predicates, types);
        }
    }
}

std::unique_ptr<RuleIndex> SchemaParser::parseIndex() {
    Rule::Predicates predicates;
    eckit::StringDict types;
    RuleIndex::Child rule;

    consume('[');

    const std::size_t line = line_ + 1;

    char c = peek();
    if (c == ']') {
        consume(c);
        return std::make_unique<RuleIndex>(line, predicates, types, std::move(rule));
    }

    for (;;) {

        c = peek();

        if (c == '[') {
            rule = parseDatum();
        }
        else {
            predicates.emplace_back(parsePredicate(types));
            while ((c = peek()) == ',') {
                consume(c);
                predicates.emplace_back(parsePredicate(types));
            }
        }

        c = peek();
        if (c == ']') {
            consume(c);
            return std::make_unique<RuleIndex>(line, predicates, types, std::move(rule));
        }
    }
}

std::unique_ptr<RuleDatabase> SchemaParser::parseDatabase() {
    Rule::Predicates predicates;
    eckit::StringDict types;
    RuleDatabase::Children rules;

    consume('[');

    const std::size_t line = line_ + 1;

    char c = peek();
    if (c == ']') {
        consume(c);
        return std::make_unique<RuleDatabase>(line, predicates, types, rules);
    }

    for (;;) {

        c = peek();

        if (c == '[') {
            rules.emplace_back(parseIndex());
        }
        else {
            predicates.emplace_back(parsePredicate(types));
            while ((c = peek()) == ',') {
                consume(c);
                predicates.emplace_back(parsePredicate(types));
            }
        }

        c = peek();
        if (c == ']') {
            consume(c);
            return std::make_unique<RuleDatabase>(line, predicates, types, rules);
        }
    }
}

void SchemaParser::parse(RuleList& result, TypesRegistry& registry) {
    eckit::StringDict types;

    parseTypes(types);
    for (const auto& [keyword, type] : types) {
        registry.addType(keyword, type);
    }

    char c;
    while ((c = peek()) == '[') {
        result.emplace_back(parseDatabase());
    }

    if (c) {
        throw StreamParser::Error(std::string("Error parsing rules: remaining char: ") + c);
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

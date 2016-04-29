/*
 * (C) Copyright 1996-2016 ECMWF.
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


#include "fdb5/SchemaParser.h"
#include "fdb5/Rule.h"
#include "fdb5/Predicate.h"
#include "fdb5/MatchAlways.h"
#include "fdb5/MatchAny.h"
#include "fdb5/MatchValue.h"
#include "fdb5/MatchOptional.h"
#include "fdb5/MatchHidden.h"
#include "fdb5/TypesRegistry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

std::string SchemaParser::parseIdent(bool emptyOK) {
    std::string s;
    for (;;) {
        char c = peek();
        switch (c) {
        case 0:
        case '/':
        case '=':
        case ',':
        case '-':
        case ';':
        case ':':
        case '[':
        case ']':
        case '?':
            if (s.empty() && !emptyOK) {
                throw StreamParser::Error("Syntax error (possible trailing comma)", line_ + 1);
            }
            return s;

        default:
            consume(c);
            s += c;
            break;
        }
    }
}

Predicate *SchemaParser::parsePredicate(std::map<std::string, std::string> &types) {

    std::set<std::string> values;
    std::string k = parseIdent();

    char c = peek();

    if (c == ':') {
        consume(c);
        ASSERT(types.find(k) == types.end());
        types[k] = parseIdent();
        c = peek();
    }

    if (c == '?') {
        consume(c);
        return new Predicate(k, new MatchOptional(parseIdent(true)));
    }

    if (c == '-') {
        consume(c);
        if (types.find(k) == types.end()) {
            // Register ignore type
            types[k] = "Ignore";
        }
        return new Predicate(k, new MatchHidden(parseIdent(true)));
    }

    if (c != ',' && c != '[' && c != ']') {
        consume("=");

        values.insert(parseIdent());

        while ((c = peek()) == '/') {
            consume(c);
            values.insert(parseIdent());
        }
    }

    switch (values.size()) {
    case 0:
        return new Predicate(k, new MatchAlways());
        break;

    case 1:
        return new Predicate(k, new MatchValue(*values.begin()));
        break;

    default:
        return new Predicate(k, new MatchAny(values));
        break;
    }
}

void SchemaParser::parseTypes(std::map<std::string, std::string> &types) {
    for (;;) {
        std::string name = parseIdent(true);
        if (name.empty()) {
            break;
        }
        consume(':');
        std::string type = parseIdent();
        consume(';');
        ASSERT(types.find(name) == types.end());
        types[name] = type;
    }
}

Rule *SchemaParser::parseRule(const Schema &owner) {
    std::vector<Predicate *> predicates;
    std::vector<Rule *> rules;
    std::map<std::string, std::string> types;

    consume('[');

    size_t line = line_ + 1;

    char c = peek();
    if (c == ']') {
        consume(c);
        return new Rule(owner, line, predicates, rules, types);
    }


    for (;;) {

        char c = peek();

        if ( c == '[') {
            while ( c == '[') {
                rules.push_back(parseRule(owner));
                c = peek();
            }
        } else {
            predicates.push_back(parsePredicate(types));
            while ( (c = peek()) == ',') {
                consume(c);
                predicates.push_back(parsePredicate(types));
            }
        }

        c = peek();
        if (c == ']') {
            consume(c);
            return new Rule(owner, line, predicates, rules, types);
        }


    }
}

SchemaParser::SchemaParser(std::istream &in) : StreamParser(in, true) {
}

void SchemaParser::parse(const Schema &owner,
                         std::vector<Rule *> &result, TypesRegistry &registry) {
    char c;
    std::map<std::string, std::string> types;

    parseTypes(types);
    for (std::map<std::string, std::string>::const_iterator i = types.begin(); i != types.end(); ++i) {
        registry.addType(i->first, i->second);
    }

    while ((c = peek()) == '[') {
        result.push_back(parseRule(owner));
    }
    if (c) {
        throw StreamParser::Error(std::string("Error parsing rules: remaining char: ") + c);
    }
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace eckit

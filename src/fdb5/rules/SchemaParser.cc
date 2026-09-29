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
#include "eckit/exception/Exceptions.h"
#include "eckit/parser/StreamParser.h"
#include "fdb5/api/exceptions/SchemaError.h"
#include "fdb5/rules/ExcludeAll.h"
#include "fdb5/rules/MatchAlways.h"
#include "fdb5/rules/MatchAny.h"
#include "fdb5/rules/MatchHidden.h"
#include "fdb5/rules/MatchOptional.h"
#include "fdb5/rules/MatchValue.h"
#include "fdb5/rules/Predicate.h"
#include "fdb5/rules/Rule.h"

#include <fstream>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <utility>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

SchemaParser::SchemaParser(const eckit::PathName& path) :
    path_(std::make_tuple<>(path, std::ifstream(path.localPath()))) {

    if (!std::get<1>(*path_)) {
        auto ex = eckit::CantOpenFile(path);
        ex.dumpStackTrace();
        throw ex;
    }

    parser_ = std::make_unique<eckit::StreamParser>(std::get<1>(*path_), true);
}

SchemaParser::SchemaParser(std::istream& in) :
    path_(std::nullopt), parser_(std::make_unique<eckit::StreamParser>(in, true)) {}

bool SchemaParser::isAscii(char c) {
    return static_cast<unsigned char>(c) < 128;
}

char SchemaParser::peek(bool spaces) {
    const char peeked = parser_->peek(spaces);

    if (peeked != 0 && !isAscii(peeked)) {
        std::stringstream buf;
        buf << "Schema file contained non-ASCII character which are not supported." << std::endl;
        throw SchemaError(getSchemaPath(), buf.str(), parser_->line() + 1);
    }

    return peeked;
}

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
                    throw SchemaError(getSchemaPath(), "Syntax error: found '" + std::string{c} + "'",
                                      parser_->line() + 1);
                }
                return s;
            case '-':
                if (s.empty() && !emptyOK) {
                    throw SchemaError(getSchemaPath(), "Syntax error: found '-'", parser_->line() + 1);
                }
                if (!value) {
                    return s;
                }
                [[fallthrough]];
            default:
                parser_->consume(c);
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
        parser_->consume(c);
        ASSERT(types.find(k) == types.end());
        types[k] = parseIdent(false, false);
        c = peek();
    }

    if (c == '?') {
        parser_->consume(c);
        return std::make_unique<Predicate>(k, new MatchOptional(parseIdent(true, true)));
    }

    if (c == '-') {
        parser_->consume(c);
        if (types.find(k) == types.end()) {
            // Register ignore type
            types[k] = "Ignore";
        }
        return std::make_unique<Predicate>(k, new MatchHidden(parseIdent(true, true)));
    }

    if (c != ',' && c != '[' && c != ']') {
        parser_->consume("=");

        std::string val = parseIdent(true, false);
        exclude = val[0] == '!';

        if (exclude) {
            values.insert(val.substr(1));
        }
        else {
            values.insert(val);
        }

        while ((c = peek()) == '/') {
            parser_->consume(c);
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

    try {
        for (;;) {
            const auto name = parseIdent(false, true);
            if (name.empty()) {
                break;
            }
            parser_->consume(':');
            const auto type = parseIdent(false, false);
            parser_->consume(';');
            ASSERT(types.find(name) == types.end());
            types[name] = type;
        }
    }
    catch (eckit::StreamParser::Error& spe) {
        std::stringstream buf;
        buf << "SchemaParser::parseTypes: Error during parsing of types in schema, check the definitions: '<name>: "
               "<type>;'."
            << " Underlying issue: " << spe.what();

        throw SchemaError(getSchemaPath(), buf.str(), parser_->line() + 1);
    }
}

std::unique_ptr<RuleDatum> SchemaParser::parseDatum() {
    Rule::Predicates predicates;
    eckit::StringDict types;

    parser_->consume('[');

    const std::size_t line = parser_->line() + 1;

    char c = peek();
    if (c == ']') {
        parser_->consume(c);
        return std::make_unique<RuleDatum>(line, predicates, types);
    }

    for (;;) {

        c = peek();

        predicates.emplace_back(parsePredicate(types));
        while ((c = peek()) == ',') {
            parser_->consume(c);
            predicates.emplace_back(parsePredicate(types));
        }

        c = peek();
        if (c == ']') {
            parser_->consume(c);
            return std::make_unique<RuleDatum>(line, predicates, types);
        }
    }
}

std::unique_ptr<RuleIndex> SchemaParser::parseIndex() {
    Rule::Predicates predicates;
    eckit::StringDict types;
    RuleIndex::Child rule;

    parser_->consume('[');

    const std::size_t line = parser_->line() + 1;

    char c = peek();
    if (c == ']') {
        parser_->consume(c);
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
                parser_->consume(c);
                predicates.emplace_back(parsePredicate(types));
            }
        }

        c = peek();
        if (c == ']') {
            parser_->consume(c);
            return std::make_unique<RuleIndex>(line, predicates, types, std::move(rule));
        }
    }
}

std::unique_ptr<RuleDatabase> SchemaParser::parseDatabase() {
    Rule::Predicates predicates;
    eckit::StringDict types;
    RuleDatabase::Children rules;

    try {
        parser_->consume('[');

        const std::size_t line = parser_->line() + 1;

        char c = peek();
        if (c == ']') {
            parser_->consume(c);
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
                    parser_->consume(c);
                    predicates.emplace_back(parsePredicate(types));
                }
            }

            c = peek();
            if (c == ']') {
                parser_->consume(c);
                return std::make_unique<RuleDatabase>(line, predicates, types, rules);
            }
        }
    }
    catch (eckit::StreamParser::Error& spe) {
        std::stringstream buf;
        buf << "SchemaParser::parseDatabase: Error during parsing of rules in schema, check for closing brackets and "
               "definitions."
            << " Underlying issue: " << spe.what();

        throw SchemaError(getSchemaPath(), buf.str(), parser_->line() + 1);
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
        throw SchemaError(getSchemaPath(),
                          std::string("SchemaParser::parse: Error parsing rules: remaining char: ") + c,
                          parser_->line());
    }


    if (result.size() == 0) {
        std::stringstream buf;
        buf << "SchemaParser::parse: Empty rule list. Didn't find any rule in the provided schema file." << std::endl;
        throw SchemaError(getSchemaPath(), buf.str(), parser_->line());
    }
}

//----------------------------------------------------------------------------------------------------------------------


}  // namespace fdb5

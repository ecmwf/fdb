/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   RequestParser.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016


#include "fdb5/RequestParser.h"
#include "fdb5/Rule.h"
#include "fdb5/Predicate.h"
#include "fdb5/MatchAlways.h"
#include "fdb5/MatchAny.h"
#include "fdb5/MatchValue.h"
#include "fdb5/MatchOptional.h"
#include "fdb5/MatchHidden.h"
#include "fdb5/Handlers.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

std::string RequestParser::parseIdent() {
    std::string s;
    for (;;) {
        char c = peek();
        switch (c) {
        case 0:
        case '/':
        case '=':
        case ',':
            if (s.empty()) {
                throw StreamParser::Error("Syntax error", line_ + 1);
            }
            return s;

        default:
            consume(c);
            s += c;
            break;
        }
    }
}

RequestParser::RequestParser(std::istream &in) : StreamParser(in, true) {
}

MarsRequest RequestParser::parse() {
    char c;

    MarsRequest r(parseIdent());

    while ((c = peek()) == ',') {
        consume(c);
        std::string keyword = parseIdent();
        consume('=');

        eckit::StringList values;
        values.push_back(parseIdent());
        while ((c = peek()) == '/') {
            consume(c);
            values.push_back(parseIdent());
        }
        r.setValues(keyword, values);
    }

    if (peek()) {
        throw StreamParser::Error(std::string("Error parsing request: remaining char: ") + c);
    }

    return r;
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace eckit

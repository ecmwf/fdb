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


#include <cctype>

#include "fdb5/RequestParser.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

std::string RequestParser::parseIdent(bool lower) {
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
            if (lower) {
                s += tolower(c);
            } else {
                s += c;
            }
            break;
        }
    }
}

RequestParser::RequestParser(std::istream &in) : StreamParser(in, true) {
}

MarsRequest RequestParser::parse(bool lower) {
    char c;

    MarsRequest r(parseIdent(lower));

    while ((c = peek()) == ',') {
        consume(c);
        std::string keyword = parseIdent(lower);
        consume('=');

        eckit::StringList values;
        values.push_back(parseIdent(lower));
        while ((c = peek()) == '/') {
            consume(c);
            values.push_back(parseIdent(lower));
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

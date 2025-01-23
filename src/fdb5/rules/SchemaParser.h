/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @date Jun 2012

#ifndef fdb5_SchemaParser_h
#define fdb5_SchemaParser_h

#include <memory>

#include "eckit/parser/StreamParser.h"
#include "eckit/types/Types.h"

namespace fdb5 {

class Schema;
class Rule;
class Predicate;
class TypesRegistry;

//----------------------------------------------------------------------------------------------------------------------

class SchemaParser : public eckit::StreamParser {

public: // methods

    SchemaParser(std::istream &in);

    void parse(const Schema &owner, std::vector<std::unique_ptr<Rule>> &, TypesRegistry &registry);

private: // methods

    std::string parseIdent(bool value, bool emptyOK);

    std::unique_ptr<Rule> parseRule(const Schema &owner);

    std::unique_ptr<Predicate> parsePredicate(std::map<std::string, std::string> &types);
    void parseTypes(std::map<std::string, std::string> &);

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace eckit

#endif

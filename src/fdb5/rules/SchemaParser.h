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

#include <iosfwd>
#include <memory>
#include <string>

#include "eckit/parser/StreamParser.h"
#include "eckit/types/Types.h"

#include "fdb5/rules/Rule.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class SchemaParser : public eckit::StreamParser {

public:  // methods

    SchemaParser(std::istream& in) : StreamParser(in, true) {}

    void parse(RuleList& result, TypesRegistry& registry);

private:  // methods

    std::string parseIdent(bool value, bool emptyOK);

    std::unique_ptr<RuleDatum> parseDatum();

    std::unique_ptr<RuleIndex> parseIndex();

    std::unique_ptr<RuleDatabase> parseDatabase();

    std::unique_ptr<Predicate> parsePredicate(eckit::StringDict& types);

    void parseTypes(eckit::StringDict& types);
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

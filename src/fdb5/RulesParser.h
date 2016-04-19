/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @date Jun 2012

#ifndef fdb5_RulesParser_h
#define fdb5_RulesParser_h

#include "eckit/parser/StreamParser.h"
#include "eckit/types/Types.h"

namespace fdb5 {

class Rule;
class Predicate;

//----------------------------------------------------------------------------------------------------------------------

class RulesParser : public eckit::StreamParser {

public: // methods

    RulesParser(std::istream& in);

    void parse(std::vector<Rule*>&);

private: // methods

    std::string parseIdent(bool emptyOK = false);

    Rule* parseRule();

    Predicate* parsePredicate();

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace eckit

#endif

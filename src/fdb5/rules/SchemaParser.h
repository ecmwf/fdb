// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Baudouin Raoult
/// @date Jun 2012

#pragma once

#include <cstddef>
#include <fstream>
#include <iosfwd>
#include <memory>
#include <string>

#include "eckit/filesystem/PathName.h"
#include "eckit/parser/StreamParser.h"
#include "eckit/types/Types.h"

#include "fdb5/rules/Rule.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class SchemaParser {

public:  // methods

    explicit SchemaParser(const eckit::PathName& path);

    explicit SchemaParser(std::istream& in);

    void parse(RuleList& result, TypesRegistry& registry);

private:  // methods

    bool isAscii(char c);

    char peek(bool spaces = false);

    std::string parseIdent(bool value, bool emptyOK);

    std::unique_ptr<RuleDatum> parseDatum();

    std::unique_ptr<RuleIndex> parseIndex();

    std::unique_ptr<RuleDatabase> parseDatabase();

    std::unique_ptr<Predicate> parsePredicate(eckit::StringDict& types);

    void parseTypes(eckit::StringDict& types);

private:  // members

    std::string getSchemaPath() const noexcept {
        return path_.has_value() ? std::get<0>(*path_).localPath() : "Created from std::istream";
    }

    std::optional<std::tuple<eckit::PathName, std::ifstream>> path_;
    std::unique_ptr<eckit::StreamParser> parser_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

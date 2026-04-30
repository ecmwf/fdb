/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   FileSpace.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   June 2016

#ifndef fdb5_FileSpace_H
#define fdb5_FileSpace_H

#include <iosfwd>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "eckit/types/Types.h"
#include "eckit/utils/Regex.h"

#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/toc/Root.h"

namespace eckit {
class LocalConfiguration;
}

namespace fdb5 {

class Config;
class FileSpaceHandler;
class Key;

//----------------------------------------------------------------------------------------------------------------------

struct TocPath {
    eckit::PathName directory_;
    ControlIdentifiers controlIdentifiers_;
};

//----------------------------------------------------------------------------------------------------------------------

class FileSpace {

public:  // types

    /// Keyword-based selector for FileSpace matching.
    /// Unlike `regex:`, this allows partial requests (missing keywords) to match.
    /// Configured from a YAML `match:` block.
    ///
    /// Example YAML:
    ///   match:
    ///     class: od
    ///     expver: '0001'
    ///     stream: [scda, scwv, oper, wave, enfo, waef]
    ///
    /// For each configured keyword:
    ///   - If the input Key has that keyword (with a non-empty value), its value must match one of the configured
    ///   values.
    ///   - If the keyword is absent or empty in the Key, it is treated as a match.
    ///
    class MatchSelector {
    public:  // methods

        // MatchSelector() = default;

        explicit MatchSelector(const eckit::LocalConfiguration& cfg);

        bool empty() const { return entries_.empty(); }

        /// @returns true if @p key is compatible with the configured constraints.
        bool match(const Key& key) const;

        void print(std::ostream& s) const;

        friend std::ostream& operator<<(std::ostream& s, const MatchSelector& m) {
            m.print(s);
            return s;
        }

    private:  // members

        std::map<std::string, std::vector<std::string>> entries_;
    };

public:  // methods

    /// Construct a FileSpace using a regex to match the (stringified) key.
    FileSpace(const std::string& name, const std::string& re, const std::string& handler,
              const std::vector<Root>& roots);

    /// Construct a FileSpace using a keyword-based selector (FDB-331).
    /// Prefer this form whenever the request may be partial: a missing
    /// keyword in the incoming Key will not disqualify the FileSpace.
    FileSpace(const std::string& name, const MatchSelector& matcher, const std::string& handler,
              const std::vector<Root>& roots);

    /// Selects the filesystem from where this Key will be inserted
    /// @note This method must be idempotent -- it returns always the same value after the first call
    /// @param key is a complete identifier for the first level of the schema
    /// @param db part of the full path
    TocPath filesystem(const Config& config, const Key& key, const eckit::PathName& db) const;

    void all(eckit::StringSet&) const;
    void enabled(const ControlIdentifier& controlIdentifier, eckit::StringSet&) const;
    std::vector<eckit::PathName> enabled(const ControlIdentifier& controlIdentifier) const;

    bool match(const Key& key) const;

    friend std::ostream& operator<<(std::ostream& s, const FileSpace& x) {
        x.print(s);
        return s;
    }

    std::vector<eckit::PathName> roots() const;

private:  // methods

    bool existsDB(const Key& key, const eckit::PathName& db, TocPath& existsDB) const;

    void print(std::ostream& out) const;

private:  // members

    typedef std::vector<Root> RootVec;

    std::string name_;

    std::string handler_;

    eckit::Regex re_;
    std::optional<MatchSelector> matcher_;

    RootVec roots_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

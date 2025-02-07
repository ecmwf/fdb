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
#include <string>
#include <vector>

#include "eckit/types/Types.h"
#include "eckit/utils/Regex.h"

#include "fdb5/api/helpers/ControlIterator.h"
#include "fdb5/toc/Root.h"

namespace fdb5 {

class FileSpaceHandler;

//----------------------------------------------------------------------------------------------------------------------

struct TocPath {
    eckit::PathName directory_;
    ControlIdentifiers controlIdentifiers_;
};

//----------------------------------------------------------------------------------------------------------------------

class FileSpace {

public:  // methods

    FileSpace(const std::string& name, const std::string& re, const std::string& handler,
              const std::vector<Root>& roots);

    /// Selects the filesystem from where this Key will be inserted
    /// @note This method must be idempotent -- it returns always the same value after the first call
    /// @param key is a complete identifier for the first level of the schema
    /// @param db part of the full path
    TocPath filesystem(const Key& key, const eckit::PathName& db) const;

    void all(eckit::StringSet&) const;
    void enabled(const ControlIdentifier& controlIdentifier, eckit::StringSet&) const;
    std::vector<eckit::PathName> enabled(const ControlIdentifier& controlIdentifier) const;

    bool match(const std::string& s) const;

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

    RootVec roots_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

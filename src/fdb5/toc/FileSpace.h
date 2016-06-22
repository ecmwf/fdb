/*
 * (C) Copyright 1996-2016 ECMWF.
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
/// @date   Mar 2016

#ifndef fdb5_FileSpace_H
#define fdb5_FileSpace_H

#include <string>
#include <vector>

#include "eckit/utils/Regex.h"
#include "eckit/types/Types.h"
#include "fdb5/toc/Root.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FileSpace  {

public: // methods

    FileSpace(const std::string& name,
              const std::string& re,
              const std::string& handler,
              const std::vector<Root>& roots);

    /// Selects the filesystem from where this Key will be inserted
    /// @note This method must be idempotent -- it returns always the same value after the first call
    eckit::PathName filesystem(const Key& key) const;

    void all(eckit::StringSet&) const;
    void writable(eckit::StringSet&) const;
    void visitable(eckit::StringSet&) const;

    bool match(const std::string& s) const;

private: // methods

    std::vector<eckit::PathName> writable() const;
    std::vector<eckit::PathName> visitable() const;

    bool existsDB(const Key& key, eckit::PathName& path) const;

private: // members

    typedef std::vector<Root> RootVec;

    std::string name_;
    std::string handler_;

    eckit::Regex re_;

    RootVec roots_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

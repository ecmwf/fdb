// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   Root.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#pragma once

#include <iosfwd>
#include <string>

#include "eckit/filesystem/PathName.h"

#include "fdb5/api/helpers/ControlIterator.h"

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------


class Root {

public:  // methods

    Root(const std::string& path, const std::string& filespace, bool list, bool retrieve, bool archive, bool wipe);

    // Root(const Root&) = default;
    // Root& operator=(const Root&) = default;

    const eckit::PathName& path() const;

    /// Root exists in the filesystem, use this check to avoid errors when accessing
    /// This result is computed with a lazy approach and then memoized
    bool exists() const;

    bool enabled(const ControlIdentifier& controlIdentifier) const {
        return controlIdentifiers_.enabled(controlIdentifier);
    };

    const ControlIdentifiers& controlIdentifiers() const { return controlIdentifiers_; }

    const std::string& filespace() const;

    friend std::ostream& operator<<(std::ostream& s, const Root& x) {
        x.print(s);
        return s;
    }

private:  // methods

    void print(std::ostream& out) const;

private:  // members

    eckit::PathName path_;

    std::string filespace_;
    mutable bool checked_;
    mutable bool exists_;

    ControlIdentifiers controlIdentifiers_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5


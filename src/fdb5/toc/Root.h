/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Root.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_Root_H
#define fdb5_Root_H

#include <string>
#include <iosfwd>

#include "eckit/filesystem/PathName.h"

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class Root  {

public: // methods

    Root(const std::string& path,
         const std::string& filespace,
         bool writable = true,
         bool visit  = true);

    const eckit::PathName &path() const;

    /// Root exists in the filesystem, use this check to avoid errors when accessing
    /// This result is cached at construction
    bool exists() const {  return exists_; }

    /// Root is in use, when archiving
    bool writable() const { return writable_; }

    /// Root is visited, when retrieving
    bool visit() const { return visit_; }

    const std::string& filespace() const;

    friend std::ostream& operator<<(std::ostream &s, const Root& x) {
        x.print(s);
        return s;
    }

private: // methods

    void print( std::ostream &out ) const;

private: // members

    eckit::PathName path_;

    std::string filespace_;

    bool writable_;
    bool visit_;
    bool exists_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   Permission.h
/// @author Emanuele Danovaro
/// @date   May 2022

#pragma once

#include <string>
#include <iosfwd>

#define TOC_PERMISSION_LIST 1
#define TOC_PERMISSION_RETRIEVE 2
#define TOC_PERMISSION_ARCHIVE 4
#define TOC_PERMISSION_WIPE 8

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class Permission {

public: // methods

    Permission() :
        permission_(TOC_PERMISSION_LIST | TOC_PERMISSION_RETRIEVE | TOC_PERMISSION_ARCHIVE | TOC_PERMISSION_WIPE) {}

    Permission(bool list, bool retrieve, bool archive, bool wipe) :
        permission_(
            (list ? TOC_PERMISSION_LIST : 0) |
            (retrieve ? TOC_PERMISSION_RETRIEVE : 0) |
            (archive ? TOC_PERMISSION_ARCHIVE : 0) |
            (wipe ? TOC_PERMISSION_WIPE : 0)) {}

    Permission(const Permission&) = default;
    Permission& operator=(const Permission&) = default;

    /// Root is visited, when listing
    bool list() const { return (permission_ & TOC_PERMISSION_LIST); }
    /// Root is visited, when retrieving
    bool retrieve() const { return (permission_ & TOC_PERMISSION_RETRIEVE); }

    /// Root is in use, when archiving
    bool archive() const { return (permission_ & TOC_PERMISSION_ARCHIVE); }
    /// Root is in use, when wiping
    bool wipe() const { return (permission_ & TOC_PERMISSION_WIPE); }

    friend std::ostream& operator<<(std::ostream &s, const Permission& x) {
        x.print(s);
        return s;
    }

private: // methods

    void print( std::ostream &out ) const {
        out << "Permission("
        << "list=" << list()
        << ",retrieve=" << retrieve()
        << ",archive=" << archive()
        << ",wipe=" << wipe()
        <<")";
    }

private: // members

    char permission_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

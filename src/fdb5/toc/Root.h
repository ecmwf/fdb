/*
 * (C) Copyright 1996-2017 ECMWF.
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

    bool writable() const; ///< Root is in use, when archiving
    bool visit() const;    ///< Root is visited, when retrievind

    const std::string& filespace() const;

private: // members

    eckit::PathName path_;

    std::string filespace_;

    bool writable_;
    bool visit_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

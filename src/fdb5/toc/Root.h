/*
 * (C) Copyright 1996-2016 ECMWF.
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

#include "eckit/utils/Regex.h"
#include "eckit/filesystem/PathName.h"

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class Root  {

public: // methods

    Root(const std::string& re,
         const std::string& path,
         const std::string& handler,
         bool active = true,
         bool visit  = true);

    bool match(const std::string &s) const;

    const eckit::PathName &path() const;

    bool active() const; ///< Root is in use, when archiving
    bool visit() const;  ///< Root is visited, when retrievind

private: // members

    eckit::Regex re_;
    eckit::PathName path_;

    std::string handler_;

    bool active_;
    bool visit_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

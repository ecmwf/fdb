/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @file   PoolEntry.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_PoolEntry_H
#define fdb5_PoolEntry_H

#include "eckit/filesystem/PathName.h"

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class PoolEntry {

public:  // methods

    PoolEntry(const std::string& path, const std::string& poolgroup, bool writable = true, bool visit = true);

    const eckit::PathName& path() const;

    bool writable() const;  ///< PoolEntry is in use, when archiving
    bool visit() const;     ///< PoolEntry is visited, when retrievind

    const std::string& poolgroup() const;

private:  // members

    eckit::PathName path_;

    std::string poolgroup_;

    bool writable_;
    bool visit_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

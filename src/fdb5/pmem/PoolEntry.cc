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

#include "fdb5/pmem/PoolEntry.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

PoolEntry::PoolEntry(const std::string &path, const std::string& poolgroup, bool active, bool visit):
    path_(path),
    poolgroup_(poolgroup),
    writable_(active),
    visit_(visit) {

}

const eckit::PathName& PoolEntry::path() const {
    return path_;
}

bool PoolEntry::writable() const {
    return writable_;
}

bool PoolEntry::visit() const {
    return visit_;
}

const std::string& PoolEntry::poolgroup() const
{
    return poolgroup_;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

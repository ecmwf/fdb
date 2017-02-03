/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @author Tiago Quintino
/// @date Nov 2016

#include <typeinfo>

#include "fdb5/toc/TocIndexLocation.h"

using namespace eckit;


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


TocIndexLocation::TocIndexLocation(const eckit::PathName& path, off_t offset) :
    path_(path),
    offset_(offset) {}


off_t TocIndexLocation::offset() const {
    return offset_;
}


const PathName& TocIndexLocation::path() const {
    return path_;
}

PathName TocIndexLocation::url() const
{
    return path_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

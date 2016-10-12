/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/toc/KeyBasedFileSpaceHandler.h"

#include "fdb5/toc/FileSpace.h"
#include "fdb5/database/Key.h"

using eckit::Log;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

KeyBasedFileSpaceHandler::~KeyBasedFileSpaceHandler() {
}

eckit::PathName KeyBasedFileSpaceHandler::selectFileSystem(const Key& key, const FileSpace& fs) const {

    // check if key is mapped already to a filesystem



    // if not, assign a filesystem



    // append key mapping to record file


}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

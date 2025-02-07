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

/// @author Simon Smart
/// @date   November 2018

#ifndef fdb5_api_DumpIterator_H
#define fdb5_api_DumpIterator_H

#include <string>

#include "fdb5/api/helpers/APIIterator.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// Define a standard object which can be used to iterate the results of a
/// dump() call on an arbitrary FDB object

using DumpElement = std::string;

using DumpIterator = APIIterator<DumpElement>;

using DumpAggregateIterator = APIAggregateIterator<DumpElement>;

using DumpAsyncIterator = APIAsyncIterator<DumpElement>;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

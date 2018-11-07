/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date   November 2018

#ifndef fdb5_api_DumpIterator_H
#define fdb5_api_DumpIterator_H

#include "fdb5/api/helpers/APIIterator.h"

#include <string>

/*
 * Define a standard object which can be used to iterate the results of a
 * dump() call on an arbitrary FDB object
 */

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

using DumpIterator = APIIterator<std::string>;

using DumpAggregateIterator = APIAggregateIterator<std::string>;

using DumpAsyncIterator = APIAsyncIterator<std::string>;

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

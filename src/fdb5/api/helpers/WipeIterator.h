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

#ifndef fdb5_api_WipeIterator_H
#define fdb5_api_WipeIterator_H

#include "fdb5/api/helpers/APIIterator.h"

#include <string>

/*
 * Define a standard object which can be used to iterate the results of a
 * wipe() call on an arbitrary FDB object
 */

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

using WipeElement = std::string;

using WipeIterator = APIIterator<WipeElement>;

using WipeAggregateIterator = APIAggregateIterator<WipeElement>;

using WipeAsyncIterator = APIAsyncIterator<WipeElement>;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

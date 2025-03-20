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

#pragma once

#include "fdb5/api/helpers/ControlIterator.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

using StatusElement = ControlElement;

using StatusIterator = APIIterator<StatusElement>;

using StatusAggregateIterator = APIAggregateIterator<StatusElement>;

using StatusAsyncIterator = APIAsyncIterator<StatusElement>;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
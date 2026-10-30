// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Simon Smart
/// @date   November 2018

#pragma once

#include "fdb5/api/helpers/ControlIterator.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

using StatusElement = ControlElement;

using StatusIterator = APIIterator<StatusElement>;

using StatusAggregateIterator = APIAggregateIterator<StatusElement>;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5
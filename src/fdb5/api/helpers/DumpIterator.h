// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Simon Smart
/// @date   November 2018

#pragma once

#include <string>

#include "fdb5/api/helpers/APIIterator.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// Define a standard object which can be used to iterate the results of a
/// dump() call on an arbitrary FDB object

using DumpElement = std::string;

using DumpIterator = APIIterator<DumpElement>;

using DumpAggregateIterator = APIAggregateIterator<DumpElement>;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

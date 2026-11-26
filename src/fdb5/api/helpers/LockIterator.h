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

#include "eckit/filesystem/PathName.h"

#include "fdb5/api/helpers/APIIterator.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// Define a standard object which can be used to iterate the results of a
/// where() call on an arbitrary FDB object

/// @todo We might expand this to include host, port, etc, in an explicit where object.
///       Or to use eckit::URL, ...

using WhereElement = eckit::PathName;

using WhereIterator = APIIterator<WhereElement>;

using WhereAggregateIterator = APIAggregateIterator<WhereElement>;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

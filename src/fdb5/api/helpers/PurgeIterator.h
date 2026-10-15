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

using PurgeElement = std::string;

using PurgeIterator = APIIterator<PurgeElement>;

using PurgeAggregateIterator = APIAggregateIterator<PurgeElement>;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5


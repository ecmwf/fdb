// SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   Compare.h
/// @author Stefanie Reuter
/// @author Philipp Geier
/// @date   August 2025

#pragma once
#include "fdb5/tools/compare/common/Types.h"

namespace compare::mars {

/// Compare the mars keys of two messages from different locations.
///
/// @param i1  Location of the message in the first fdb
/// @param i2  Location of the message in the second fdb
/// @param opts Additional options - notably marsReqDiff is used to lookup and compare requests
/// @return Comparison result. As mars is not comparing data, only matched is expected to be set
Result compareMarsKeys(const DataIndex& i1, const DataIndex& i2, const Options& opts);

}  // namespace compare::mars

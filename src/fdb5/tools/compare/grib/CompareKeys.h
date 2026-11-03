// SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   CompareKeys.h
/// @author Stefanie Reuter
/// @author Philipp Geier
/// @date   August 2025

#pragma once

#include "Utils.h"
#include "metkit/codes/api/CodesAPI.h"

#include <string>
#include <unordered_set>

namespace compare::grib {

//---------------------------------------------------------------------------------------------------------------------

/// Compares two grib messages key by key
/// @param h1 Codes handle from the first FDB
/// @param h2 Codes handle from the second FDB
/// @param ignoreList List of keys to ignore in the comparison. Will be ignored if matchList is passed.
/// @param matchList List of specific keys that should be compared. If no key is contained, all keys will be used and
///         the ignoreList will be applied
/// @return  Comparison result
CompareResult compareKeyByKey(const metkit::codes::CodesHandle& h1, const metkit::codes::CodesHandle& h2,
                              const std::unordered_set<std::string>& ignoreList,
                              const std::unordered_set<std::string>& matchList);

//---------------------------------------------------------------------------------------------------------------------

}  // namespace compare::grib

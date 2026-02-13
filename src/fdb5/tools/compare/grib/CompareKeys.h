/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

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
/// @param hRef Reference codes handle
/// @param hTest Test codes handle
/// @param ignoreList List of keys to ignore in the comparison. Will be ignored if matchList is passed.
/// @param matchList List of specific keys that should be compared. If no key is contained, all keys will be used and
/// 		the ignoreList will be applied
/// @return  Comparison result
CompareResult compareKeyByKey(const metkit::codes::CodesHandle& hRef, const metkit::codes::CodesHandle& hTest,
                              const std::unordered_set<std::string>& ignoreList,
                              const std::unordered_set<std::string>& matchList);

//---------------------------------------------------------------------------------------------------------------------

}  // namespace compare::grib

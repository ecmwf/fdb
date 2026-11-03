// SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   CompareHash.h
/// @author Stefanie Reuter
/// @author Philipp Geier
/// @date   August 2025

#pragma once

#include "Utils.h"

#include "metkit/codes/api/CodesAPI.h"


namespace compare::grib {

/// Compares the MD5 hashes of each section of two grib messages.
/// @param h1 Reference codes handle
/// @param h2 Test codes handle
/// @return  Comparison result.
CompareResult compareMd5sums(const metkit::codes::CodesHandle& h1, const metkit::codes::CodesHandle& h2);

}  // namespace compare::grib

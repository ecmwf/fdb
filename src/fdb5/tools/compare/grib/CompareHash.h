/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

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

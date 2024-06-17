/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   CompareBitwise.h
/// @author Stefanie Reuter
/// @author Philipp Geier
/// @date   August 2025

#pragma once

#include "Utils.h"

#include "metkit/codes/api/CodesAPI.h"

#include <cstdint>

namespace compare::grib {

bool compareHeader(const metkit::codes::CodesHandle& hRef, const metkit::codes::CodesHandle& hTest);

/// Bit comparison without using eccodes
///
/// @param bufferRef  Grib message in bytes for the reference Grib message
/// @param bufferTest Grib message in bytes for the test Grib message
/// @param length sizeof(Buffer)
/// @return BitCompResult to indicate success, failure in any section or explicity a failure in numeric data comparison.
CompareResult bitComparison(const uint8_t* bufferRef, const uint8_t* bufferTest, size_t length);

}  // namespace compare::grib

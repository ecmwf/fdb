// SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   CompareDatasection.h
/// @author Stefanie Reuter
/// @author Philipp Geier
/// @date   August 2025

#pragma once

#include "fdb5/tools/compare/common/Types.h"

#include "metkit/codes/api/CodesAPI.h"


namespace compare::grib {

struct NumericResult {
    compare::NumericError relativeError;
    compare::NumericError absoluteError;
};

/// Compares the data section (values) of two grib messages.
///
/// @param h1 Reference codes handle
/// @param h2 Test codes handle
/// @param tolerance floating point comparison tolerance
/// @return Numeric comparison results containing information on absolute and relative error
NumericResult compareDataSection(const metkit::codes::CodesHandle& h1, const metkit::codes::CodesHandle& h2,
                                 double tolerance);

}  // namespace compare::grib

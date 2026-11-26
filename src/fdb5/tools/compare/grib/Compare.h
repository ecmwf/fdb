// SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   Compare.h
/// @author Stefanie Reuter
/// @author Philipp Geier
/// @date   August 2025

#pragma once

#include "fdb5/tools/compare/common/Types.h"

namespace compare::grib {

/// Compare grib messages from different locations.
/// The grib messages are compared on multiple levels.
/// First by comparing the header through a selected method (Bitexact, comparing section hashes, or key by key).
/// Then eventually also the data section is compared for equivalence.
/// To control floating-point comparison, a fp-tolerance can be customized in the options.
///
/// @param i1 Location of the FDB1 message
/// @param i2 Location of the FDB2 message
/// @param opts Additional options
/// @return Comparison result indicating whether the GRIB headers and, if requested, data sections match
Result compareGrib(const DataIndex& i1, const DataIndex& i2, const Options& opts);

}  // namespace compare::grib

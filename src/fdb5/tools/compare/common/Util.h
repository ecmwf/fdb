// SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <cstdint>
#include <cstring>

/// @file   Utils.h
/// @author Stefanie Reuter
/// @author Philipp Geier
/// @date   Feb 2026

namespace compare {

/// Reads an uint24 assuming big endianness
uint32_t readU24Be(const uint8_t* p);

/// Reads an uint32 assuming big endianness
uint32_t readU32Be(const uint8_t* p);

/// Reads an uint64 assuming big endianness
uint64_t readU64Be(const uint8_t* p);

}  // namespace compare

/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
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

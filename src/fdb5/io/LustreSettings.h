// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   LustreSettings.h
/// @author Tiago Quintino
/// @date   February 2022

#pragma once

#include <cstddef>

#include "eckit/filesystem/PathName.h"
namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

struct LustreStripe {

    LustreStripe(unsigned int count, size_t size) : count_(count), size_(size) {}

    unsigned int count_;
    size_t size_;
};

//----------------------------------------------------------------------------------------------------------------------

bool stripeLustre();

LustreStripe stripeIndexLustreSettings();

LustreStripe stripeDataLustreSettings();

//----------------------------------------------------------------------------------------------------------------------

int fdb5LustreapiFileCreate(const eckit::PathName& path, LustreStripe stripe);

bool fdb5LustreapiSupported();

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

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

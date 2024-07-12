/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#pragma once

#include "fdb5/database/Key.h"
#include "fdb5/database/FieldLocation.h"

namespace fdb5 {

using ArchiveCallback = std::function<void(const void* data, size_t size, const FieldLocation&)>;

static const ArchiveCallback CALLBACK_NOOP = [](const void* data, size_t size, const FieldLocation&) {};

} // namespace fdb5

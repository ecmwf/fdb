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
#include <future>
#include "fdb5/database/Key.h"
#include "fdb5/database/FieldLocation.h"

namespace fdb5 {

class FDB;

using ArchiveCallback = std::function<void(const Key& key, const void* data, size_t length, std::future<std::shared_ptr<FieldLocation>>)>;
using FlushCallback = std::function<void()>;
using ConstructorCallback = std::function<void(FDB&)>;

static const ArchiveCallback CALLBACK_NOOP = [](const Key& key, const void* data, size_t length, std::future<std::shared_ptr<FieldLocation>>) {};
static const FlushCallback CALLBACK_FLUSH_NOOP = []() {};
static const ConstructorCallback CALLBACK_CONSTRUCTOR_NOOP = [](FDB&) {};

} // namespace fdb5

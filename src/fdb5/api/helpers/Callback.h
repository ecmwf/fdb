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
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"

namespace fdb5 {

class FDB;
class CallbackRegistry;

using ArchiveCallback     = std::function<void(const Key& key, const void* data, size_t length,
                                           std::future<std::shared_ptr<const FieldLocation>>)>;
using FlushCallback       = std::function<void()>;
using ConstructorCallback = std::function<void(CallbackRegistry&)>;

static const ArchiveCallback CALLBACK_ARCHIVE_NOOP         = [](auto&&...) {};
static const FlushCallback CALLBACK_FLUSH_NOOP             = []() {};
static const ConstructorCallback CALLBACK_CONSTRUCTOR_NOOP = [](auto&&...) {};

// -------------------------------------------------------------------------------------------------

// This class provides a common interface for registering callbacks with an FDB object or a Store/Catalogue Handler.
class CallbackRegistry {
public:

    void registerFlushCallback(FlushCallback callback) { flushCallback_ = callback; }
    void registerArchiveCallback(ArchiveCallback callback) { archiveCallback_ = callback; }

protected:

    FlushCallback flushCallback_     = CALLBACK_FLUSH_NOOP;
    ArchiveCallback archiveCallback_ = CALLBACK_ARCHIVE_NOOP;
};

}  // namespace fdb5

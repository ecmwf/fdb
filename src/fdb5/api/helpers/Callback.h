// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

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

using ArchiveCallback = std::function<void(const Key& key, const void* data, size_t length,
                                           std::future<std::shared_ptr<const FieldLocation>>)>;
using FlushCallback = std::function<void()>;
using ConstructorCallback = std::function<void(CallbackRegistry&)>;

static const ArchiveCallback CALLBACK_ARCHIVE_NOOP = [](auto&&...) {};
static const FlushCallback CALLBACK_FLUSH_NOOP = []() {};
static const ConstructorCallback CALLBACK_CONSTRUCTOR_NOOP = [](auto&&...) {};

// -------------------------------------------------------------------------------------------------

struct Callbacks {
    FlushCallback flushCallback_ = CALLBACK_FLUSH_NOOP;
    ArchiveCallback archiveCallback_ = CALLBACK_ARCHIVE_NOOP;
};

// This class provides a common interface for registering callbacks with an FDB object or a Store/Catalogue Handler.
class CallbackRegistry {
public:

    virtual ~CallbackRegistry() = default;

    virtual void registerFlushCallback(FlushCallback callback) = 0;
    virtual void registerArchiveCallback(ArchiveCallback callback) = 0;
};

}  // namespace fdb5

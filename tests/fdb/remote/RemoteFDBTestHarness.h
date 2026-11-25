// SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "fdb5/api/RemoteFDB.h"

#include "eckit/exception/Exceptions.h"

#include <memory>
#include <mutex>

namespace fdb5 {

// Friend of RemoteFDB (see RemoteFDB.h): registers a pending-request queue for a synthetic requestID without
// contacting the server, so tests can drive the client deterministically.
class RemoteFDBTestHarness {
public:

    static std::shared_ptr<RemoteFDB::MessageQueue> registerQueue(RemoteFDB& fdb) {
        uint32_t requestID = fdb.generateRequestID();
        return fdb.requests_.add(requestID, 320).release();
    }
};

}  // namespace fdb5

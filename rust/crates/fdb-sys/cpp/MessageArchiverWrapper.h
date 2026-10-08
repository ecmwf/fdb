// fdb MessageArchiver bridge — wraps `fdb5::MessageArchiver`.
#pragma once

#include "Types.h"

#include "eckit-sys/src/lib.rs.h"

#include "fdb5/message/MessageArchiver.h"

#include <cstdint>
#include <memory>

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

/// Wraps `fdb5::MessageArchiver` — used by mars-client-cpp's
/// `FDBBase::archive`. The C++ ctor takes `(key, completeTransfers, verbose,
/// config)`; the wrapper exposes all four so the caller picks values
/// (mars-client-cpp uses an empty key + both flags `false`).
class MessageArchiverWrapper {
    fdb5::MessageArchiver archiver_;

public:

    MessageArchiverWrapper(const KeyData& key, bool complete_transfers, bool verbose,
                           const eckit_bridge::ConfigWrapper& config);

    /// `fdb5::MessageArchiver::archive(eckit::DataHandle&)` — returns bytes
    /// archived (eckit::Length cast to int64).
    int64_t archive(eckit_bridge::DataHandleWrapper& source);

    /// `fdb5::MessageArchiver::flush()`.
    void flush();

    // ============== Factories ==============

    static std::unique_ptr<MessageArchiverWrapper> create(const KeyData& key, bool complete_transfers, bool verbose,
                                                          const eckit_bridge::ConfigWrapper& config);
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge

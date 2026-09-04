// fdb MessageArchiver bridge — implementation.

#include "fdb_exceptions.h"

#include "Key.h"
#include "MessageArchiverWrapper.h"
#include "fdb-sys/src/lib.rs.h"

#include "fdb5/config/Config.h"

namespace fdb_bridge {

//----------------------------------------------------------------------------------------------------------------------

MessageArchiverWrapper::MessageArchiverWrapper(const KeyData& key, bool complete_transfers, bool verbose,
                                               const eckit_bridge::ConfigWrapper& config) :
    archiver_(Key::from_data(key), complete_transfers, verbose, fdb5::Config(config.inner())) {}

int64_t MessageArchiverWrapper::archive(eckit_bridge::DataHandleWrapper& source) {
    auto length = archiver_.archive(source.inner());
    return static_cast<int64_t>(static_cast<long long>(length));
}

void MessageArchiverWrapper::flush() {
    archiver_.flush();
}

//----------------------------------------------------------------------------------------------------------------------

std::unique_ptr<MessageArchiverWrapper> MessageArchiverWrapper::create(const KeyData& key, bool complete_transfers,
                                                                       bool verbose,
                                                                       const eckit_bridge::ConfigWrapper& config) {
    return std::make_unique<MessageArchiverWrapper>(key, complete_transfers, verbose, config);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb_bridge

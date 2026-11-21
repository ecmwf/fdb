#include "fdb5/remote/client/ClientConnectionRouter.h"

#include "fdb5/remote/client/Client.h"
#include "fdb5/remote/client/ClientConnection.h"

#include "eckit/config/Configuration.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/log/Log.h"
#include "eckit/net/Endpoint.h"

#include <unistd.h>

#include <cstddef>
#include <cstdlib>
#include <exception>
#include <memory>
#include <mutex>
#include <ostream>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace {

class ConnectionError : public eckit::Exception {
public:

    ConnectionError();
    ConnectionError(const eckit::net::Endpoint&);
    explicit ConnectionError(const std::string& detail);

    bool retryOnClient() const override { return true; }
};

ConnectionError::ConnectionError() {
    std::ostringstream s;
    s << "Unable to create a connection with the FDB server";
    reason(s.str());
    eckit::Log::status() << what() << std::endl;
}

ConnectionError::ConnectionError(const std::string& detail) {
    std::ostringstream s;
    s << "Unable to create a connection with the FDB server: " << detail;
    reason(s.str());
    eckit::Log::status() << what() << std::endl;
}

ConnectionError::ConnectionError(const eckit::net::Endpoint& endpoint) {
    std::ostringstream s;
    s << "Unable to create a connection with the FDB endpoint " << endpoint;
    reason(s.str());
    eckit::Log::status() << what() << std::endl;
}
}  // namespace

namespace fdb5::remote {

size_t random(size_t const max) {
    thread_local std::mt19937 rndGen{std::random_device{}()};
    thread_local std::uniform_int_distribution<size_t> dist;
    return dist(rndGen, std::uniform_int_distribution<size_t>::param_type{0, max});
}

//----------------------------------------------------------------------------------------------------------------------

ClientConnectionRouter::Key ClientConnectionRouter::key(const eckit::net::Endpoint& endpoint,
                                                       const std::string& defaultEndpoint) {
    // the default endpoint is part of the identity of a connection: it is used to build the field locations
    return {std::string(endpoint), defaultEndpoint};
}

std::shared_ptr<ClientConnection> ClientConnectionRouter::cached(const Key& key) {
    if (const auto iter = connections_.find(key); iter != connections_.end()) {
        // a dead or retired connection is never handed out
        if (auto conn = iter->second.lock(); conn && conn->usable()) {
            return conn;
        }
    }
    return nullptr;
}

std::shared_ptr<ClientConnection> ClientConnectionRouter::obtain(const eckit::Configuration& config,
                                                                 const eckit::net::Endpoint& endpoint,
                                                                 const std::string& defaultEndpoint,
                                                                 bool singleAttempt) {
    const auto k = key(endpoint, defaultEndpoint);
    {
        std::lock_guard lock(connectionMutex_);
        reap();
        if (auto conn = cached(k)) {
            return conn;
        }
    }

    // Connect without the lock: a slow or unresponsive server must not block the connections to the other servers.
    // n.b. throws if the server rejects the connection.
    auto connection = std::make_shared<ClientConnection>(endpoint, defaultEndpoint);
    if (!connection->connect(config, singleAttempt)) {
        return nullptr;
    }

    {
        std::lock_guard lock(connectionMutex_);
        if (auto other = cached(k)) {
            // another thread connected meanwhile: use its connection, so that there is one per endpoint
            connection->teardown();  // n.b. it has no client
            return other;
        }
        connections_.insert_or_assign(k, connection);
    }
    return connection;
}

std::shared_ptr<ClientConnection> ClientConnectionRouter::connection(const eckit::Configuration& config,
                                                                     const eckit::net::Endpoint& endpoint,
                                                                     const std::string& defaultEndpoint) {
    if (auto conn = obtain(config, endpoint, defaultEndpoint, false)) {
        return conn;
    }
    throw ConnectionError(endpoint);
}

std::shared_ptr<ClientConnection> ClientConnectionRouter::connection(
    const eckit::Configuration& config, const std::vector<std::pair<eckit::net::Endpoint, std::string>>& endpoints) {

    std::vector<std::pair<eckit::net::Endpoint, std::string>> fullEndpoints{endpoints};

    // A server that explicitly rejects the connection (e.g. protocol or configuration mismatch) will not accept it on a
    // retry, unlike a server that cannot be reached, does not answer in time or fails otherwise: the first rejection
    // is kept, to be reported as such if every endpoint rejected the connection
    std::exception_ptr firstRejection;
    std::string firstRejectionReason;
    std::string firstFailureReason;  // transient: not reached, timeout, transport error
    bool transient = false;

    while (fullEndpoints.size() > 0) {
        // select a random endpoint
        size_t idx = random(fullEndpoints.size() - 1);
        const auto [endpoint, defaultEndpoint] = fullEndpoints.at(idx);

        try {
            if (auto conn = obtain(config, endpoint, defaultEndpoint, true)) {
                return conn;
            }
            transient = true;  // not reached
        }
        catch (const RemoteFDBRejected& e) {
            eckit::Log::warning() << "FDB endpoint " << endpoint << " rejected the connection: " << e.what()
                                  << std::endl;
            if (!firstRejection) {
                firstRejection = std::current_exception();
                firstRejectionReason = e.what();
            }
        }
        catch (const std::exception& e) {
            // e.g. no answer in time, transport error: may work later
            eckit::Log::warning() << "Unable to connect to FDB endpoint " << endpoint << ": " << e.what()
                                  << std::endl;
            transient = true;
            if (firstFailureReason.empty()) {
                firstFailureReason = e.what();
            }
        }

        // unable to connect to "endpoint", remove it and try again
        if (idx != fullEndpoints.size() - 1) {  // swap with the last element
            fullEndpoints[idx] = std::move(fullEndpoints.back());
        }
        fullEndpoints.pop_back();
    }

    // no more available endpoints, we have to give up
    if (firstRejection && !transient) {
        // all the endpoints rejected the connection: report why, as such (not as a connection error worth retrying)
        std::rethrow_exception(firstRejection);
    }

    // some may work later: worth retrying, with the reasons
    std::string reasons;
    if (!firstFailureReason.empty()) {
        reasons = firstFailureReason;
    }
    if (!firstRejectionReason.empty()) {
        reasons += (reasons.empty() ? "" : "; ") + std::string("one rejected the connection: ") + firstRejectionReason;
    }
    if (reasons.empty()) {
        throw ConnectionError();
    }
    throw ConnectionError("no endpoint available: " + reasons);
}

std::shared_ptr<ClientConnection> ClientConnectionRouter::refresh(const eckit::Configuration& config,
                                                                  const std::shared_ptr<ClientConnection>& connection) {
    // Another client may already have refreshed this endpoint: the live connection is reused
    if (auto conn = obtain(config, connection->controlEndpoint(), connection->defaultEndpoint(), false)) {
        return conn;
    }
    throw ConnectionError(connection->controlEndpoint());
}

void ClientConnectionRouter::reap() {
    for (auto iter = connections_.begin(); iter != connections_.end();) {
        auto conn = iter->second.lock();
        if (!conn || !conn->usable()) {
            iter = connections_.erase(iter);
        }
        else {
            ++iter;
        }
    }
}

void ClientConnectionRouter::deregister(ClientConnection& connection) {
    std::lock_guard lock(connectionMutex_);
    const auto iter = connections_.find(key(connection.controlEndpoint(), connection.defaultEndpoint()));
    if (iter != connections_.end()) {
        auto conn = iter->second.lock();
        if (!conn || conn.get() == &connection) {
            connections_.erase(iter);
        }
    }
}

ClientConnectionRouter::ClientConnectionRouter() {}

ClientConnectionRouter& ClientConnectionRouter::instance() {
    // Leaked deliberately: avoids racing this destructor against other threads tearing
    // down connections during static/process deinitialisation.
    static ClientConnectionRouter& router = *new ClientConnectionRouter();
    return router;
}

ClientConnectionRouter::~ClientConnectionRouter() {
    // there is nothing to tear down here; as a courtesy we notify the server for those.
    std::lock_guard lock(connectionMutex_);
    for (auto& [endp, weak] : connections_) {
        if (auto conn = weak.lock()) {
            conn->teardown();
        }
    }
    connections_.clear();
}

}  // namespace fdb5::remote

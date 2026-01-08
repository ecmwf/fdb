#include <cstdlib>
#include <ctime>

#include "eckit/config/Resource.h"
#include "eckit/io/Buffer.h"
#include "eckit/log/Log.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/utils/Literals.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/api/RemoteFDB.h"
#include "fdb5/api/helpers/ListElement.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/Inspector.h"

#include "fdb5/remote/RemoteFieldLocation.h"
#include "fdb5/remote/client/Client.h"
#include "fdb5/remote/client/ClientConnectionRouter.h"
#include "fdb5/remote/client/ReadLimiter.h"

using namespace fdb5::remote;
using namespace eckit;
using namespace eckit::literals;

//----------------------------------------------------------------------------------------------------------------------

namespace fdb5 {

class RemoteFDBClient : public std::enable_shared_from_this<RemoteFDBClient>, public remote::Client {

public:  // types

    using MessageQueue = eckit::Queue<eckit::Buffer>;

public:  // method

    RemoteFDBClient(const Config& config);
    void init(std::vector<std::string>& stores, std::vector<std::string>& fieldLocationEndpoints);

    const eckit::Configuration& clientConfig() const override;

    const eckit::net::Endpoint& storeEndpoint() const;
    const eckit::net::Endpoint& storeEndpoint(const eckit::net::Endpoint& fieldLocationEndpoint) const;

private:  // methods

    friend class RemoteFDB;

    template <typename HelperClass>
    auto forwardApiCall(const HelperClass& helper, const FDBToolRequest& request)
        -> APIIterator<typename HelperClass::ValueType>;

    // Client
    bool handle(remote::Message message, uint32_t requestID) override;
    bool handle(remote::Message message, uint32_t requestID, eckit::Buffer&& payload) override;

private:  // members

    const Config config_;

    std::unordered_map<eckit::net::Endpoint, eckit::net::Endpoint> storesReadMapping_;
    std::vector<std::pair<eckit::net::Endpoint, eckit::net::Endpoint>> storesArchiveMapping_;
    std::vector<eckit::net::Endpoint> storesLocalFields_;

    // Where do we put received messages
    // @note This is a map of requestID:MessageQueue. At the point that a request is
    // complete, errored or otherwise killed, it needs to be removed from the map.
    // The shared_ptr allows this removal to be asynchronous with the actual task
    // cleaning up and returning to the client.
    std::unordered_map<uint32_t, std::shared_ptr<MessageQueue>> messageQueues_;
};

//----------------------------------------------------------------------------------------------------------------------

namespace {

template <typename T, fdb5::remote::Message msgID>
struct BaseAPIHelper {

    typedef T ValueType;

    static size_t bufferSize() { return 1_MiB; }
    static size_t queueSize() { return 100; }
    static fdb5::remote::Message message() { return msgID; }

    void encodeExtra(eckit::Stream& s) const {}
    static ValueType valueFromStream(eckit::Stream& s, const fdb5::RemoteFDBClient& client) { return ValueType(s); }
};

using StatsHelper = BaseAPIHelper<fdb5::StatsElement, fdb5::remote::Message::Stats>;

struct ListHelper : BaseAPIHelper<fdb5::ListElement, fdb5::remote::Message::List> {

    ListHelper(const int depth) : depth_(depth) {}

    static fdb5::ListElement valueFromStream(eckit::Stream& s, const fdb5::RemoteFDBClient& client) {
        fdb5::ListElement elem(s);
        std::shared_ptr<const fdb5::FieldLocation> remoteLocation;

        if (elem.hasLocation()) {

            eckit::Log::debug<fdb5::LibFdb5>() << "ListHelper::valueFromStream - original location: ";
            elem.location().dump(eckit::Log::debug<fdb5::LibFdb5>());
            eckit::Log::debug<fdb5::LibFdb5>() << std::endl;

            // TODO move the endpoint replacement to the server side ()
            if (elem.location().uri().scheme() == "fdb") {
                eckit::net::Endpoint fieldLocationEndpoint{elem.location().uri().host(), elem.location().uri().port()};

                remoteLocation =
                    fdb5::remote::RemoteFieldLocation(client.storeEndpoint(fieldLocationEndpoint),
                                                      static_cast<const RemoteFieldLocation&>(elem.location()))
                        .make_shared();
            }
            else {
                remoteLocation =
                    fdb5::remote::RemoteFieldLocation(client.storeEndpoint(), elem.location()).make_shared();
            }
        }

        return fdb5::ListElement(elem.keys(), remoteLocation, elem.timestamp());
    }

    void encodeExtra(eckit::Stream& s) const { s << depth_; }

private:

    int depth_{3};
};

struct AxesHelper : BaseAPIHelper<fdb5::AxesElement, fdb5::remote::Message::Axes> {
    AxesHelper(int level) : level_(level) {}

    void encodeExtra(eckit::Stream& s) const { s << level_; }

private:

    int level_;
};

struct InspectHelper : BaseAPIHelper<fdb5::ListElement, fdb5::remote::Message::Inspect> {

    static fdb5::ListElement valueFromStream(eckit::Stream& s, const fdb5::RemoteFDBClient& client) {
        fdb5::ListElement elem(s);

        eckit::Log::debug<fdb5::LibFdb5>() << "InspectHelper::valueFromStream - original location: ";
        elem.location().dump(eckit::Log::debug<fdb5::LibFdb5>());
        eckit::Log::debug<fdb5::LibFdb5>() << std::endl;

        if (elem.location().uri().scheme() == "fdb") {
            eckit::net::Endpoint fieldLocationEndpoint{elem.location().uri().host(), elem.location().uri().port()};

            std::shared_ptr<const fdb5::FieldLocation> remoteLocation =
                fdb5::remote::RemoteFieldLocation(client.storeEndpoint(fieldLocationEndpoint),
                                                  static_cast<const RemoteFieldLocation&>(elem.location()))
                    .make_shared();
            return fdb5::ListElement(elem.keys(), remoteLocation, elem.timestamp());
        }
        std::shared_ptr<const fdb5::FieldLocation> remoteLocation =
            fdb5::remote::RemoteFieldLocation(client.storeEndpoint(), elem.location()).make_shared();
        return fdb5::ListElement(elem.keys(), remoteLocation, elem.timestamp());
    }
};

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

RemoteFDBClient::RemoteFDBClient(const Config& config) : remote::Client(config), config_(config) {}

const eckit::Configuration& RemoteFDBClient::clientConfig() const {
    return config_;
}

void RemoteFDBClient::init(std::vector<std::string>& stores, std::vector<std::string>& fieldLocationEndpoints) {
    eckit::Buffer buf = controlWriteReadResponse(remote::Message::Stores, generateRequestID());
    eckit::MemoryStream s(buf);
    size_t numStores;
    s >> numStores;
    ASSERT(numStores > 0);

    std::unordered_set<eckit::net::Endpoint> localFields;

    for (size_t i = 0; i < numStores; i++) {
        std::string store;
        s >> store;
        size_t numAliases;
        s >> numAliases;
        std::vector<eckit::net::Endpoint> aliases;
        if (numAliases == 0) {
            eckit::net::Endpoint storeEndpoint{store};
            storesReadMapping_[storeEndpoint] = storeEndpoint;
            LOG_DEBUG_LIB(LibFdb5) << "store endpoint: " << storeEndpoint
                                   << " default data location endpoint: " << storeEndpoint << std::endl;
        }
        else {
            for (size_t j = 0; j < numAliases; j++) {
                eckit::net::Endpoint alias(s);
                if (store.empty()) {
                    storesLocalFields_.push_back(alias);
                    localFields.emplace(alias);
                }
                else {
                    eckit::net::Endpoint fieldLocationEndpoint{store};
                    storesReadMapping_[fieldLocationEndpoint] = alias;
                }
                LOG_DEBUG_LIB(LibFdb5) << "store endpoint: " << alias << " default data location endpoint: " << store
                                       << std::endl;
            }
        }
    }
    for (const auto& s : storesReadMapping_) {
        if (localFields.find(s.second) == localFields.end()) {
            storesArchiveMapping_.push_back(std::make_pair(s.second, s.first));
            stores.push_back(s.second);
            fieldLocationEndpoints.push_back(s.first);
        }
    }
    for (const auto& s : storesLocalFields_) {
        stores.push_back(s);
        fieldLocationEndpoints.push_back("");
    }
}

bool RemoteFDBClient::handle(remote::Message message, uint32_t requestID) {

    switch (message) {
        case fdb5::remote::Message::Complete: {

            auto it = messageQueues_.find(requestID);
            if (it == messageQueues_.end()) {
                return false;
            }

            it->second->close();
            // Remove entry (shared_ptr --> message queue will be destroyed when it
            // goes out of scope in the worker thread).
            messageQueues_.erase(it);
            return true;
        }
        case fdb5::remote::Message::Error: {

            std::ostringstream ss;
            ss << "RemoteFDBClient ID: " << id() << " - received an error without error description for requestID "
               << requestID << std::endl;
            throw RemoteFDBException(ss.str(), controlEndpoint());

            return false;
        }
        default:
            return false;
    }
}

bool RemoteFDBClient::handle(remote::Message message, uint32_t requestID, eckit::Buffer&& payload) {

    switch (message) {
        case fdb5::remote::Message::Blob: {
            auto it = messageQueues_.find(requestID);
            if (it == messageQueues_.end()) {
                return false;
            }

            it->second->emplace(std::move(payload));
            return true;
        }

        case fdb5::remote::Message::Error: {

            auto it = messageQueues_.find(requestID);
            if (it == messageQueues_.end()) {
                return false;
            }
            std::string msg;
            msg.resize(payload.size(), ' ');
            payload.copy(&msg[0], payload.size());
            it->second->interrupt(std::make_exception_ptr(RemoteFDBException(msg, controlEndpoint())));
            // Remove entry (shared_ptr --> message queue will be destroyed when it
            // goes out of scope in the worker thread).
            messageQueues_.erase(it);
            return true;
        }
        default:
            return false;
    }
}

// the APIRemoteAsyncIterator wraps the usual APIAsyncIterator and holds a shared_ptr<Client>,
// so if the FDB object goes out-of-scope, the client is kept alive until the iterator has been fully consumed
template <typename ValueType>
class APIRemoteAsyncIterator : public APIAsyncIterator<ValueType> {

public:  // methods

    APIRemoteAsyncIterator(std::shared_ptr<Client> client, std::function<void(eckit::Queue<ValueType>&)> workerFn,
                           size_t queueSize = 100) :
        APIAsyncIterator<ValueType>(workerFn, queueSize), client_(client) {}

private:

    std::shared_ptr<Client> client_;
};


// forwardApiCall captures the asynchronous behaviour:
//
// i) Set up a Queue to receive the messages as they come in
// ii) Encode the request+arguments and send them to the server
// iii) Return an AsyncIterator that pulls messages off the queue, and returns them to the caller.

template <typename HelperClass>
auto RemoteFDBClient::forwardApiCall(const HelperClass& helper, const FDBToolRequest& request)
    -> APIIterator<typename HelperClass::ValueType> {

    using ValueType     = typename HelperClass::ValueType;
    using IteratorType  = APIIterator<ValueType>;
    using AsyncIterator = APIRemoteAsyncIterator<ValueType>;

    // Reconnect if necessary
    refreshConnection();

    // Ensure we have an entry in the message queue before we trigger anything that
    // will result in return messages

    uint32_t id = generateRequestID();
    auto entry  = messageQueues_.emplace(id, std::make_shared<MessageQueue>(HelperClass::queueSize()));
    ASSERT(entry.second);
    std::shared_ptr<MessageQueue> messageQueue(entry.first->second);

    // Encode the request and send it to the server

    Buffer encodeBuffer(HelperClass::bufferSize());
    MemoryStream s(encodeBuffer);
    s << request;
    helper.encodeExtra(s);

    controlWriteCheckResponse(HelperClass::message(), id, true, encodeBuffer, s.position());

    // Return an AsyncIterator to allow the messages to be retrieved in the API
    const RemoteFDBClient* client = this;
    return IteratorType(
        // n.b. Don't worry about catching exceptions in lambda, as
        // this is handled in the AsyncIterator.
        new AsyncIterator(shared_from_this(), [messageQueue, client](eckit::Queue<ValueType>& queue) {
            eckit::Buffer msg{0};
            while (true) {
                if (messageQueue->pop(msg) == -1) {
                    break;
                }
                else {
                    MemoryStream s(msg);
                    queue.emplace(HelperClass::valueFromStream(s, *client));
                }
            }
            // messageQueue goes out of scope --> destructed
        }));
}

const eckit::net::Endpoint& RemoteFDBClient::storeEndpoint() const {
    if (storesLocalFields_.empty()) {
        throw eckit::SeriousBug("Unable to find a store to serve local data");
    }
    return storesLocalFields_.at(std::rand() % storesLocalFields_.size());
}
const eckit::net::Endpoint& RemoteFDBClient::storeEndpoint(const eckit::net::Endpoint& fieldLocationEndpoint) const {
    // looking for an alias for the given endpoint
    auto it = storesReadMapping_.find(fieldLocationEndpoint);
    if (it == storesReadMapping_.end()) {
        std::ostringstream ss;
        ss << "Unable to find a matching endpoint. Looking for " << fieldLocationEndpoint << std::endl;
        ss << "Available endpoints:" << std::endl;
        for (auto s : storesReadMapping_) {
            ss << s.first << " --> " << s.second << std::endl;
        }
        throw eckit::SeriousBug(ss.str());
    }
    return it->second;
}

//----------------------------------------------------------------------------------------------------------------------

RemoteFDB::RemoteFDB(const eckit::Configuration& config, const std::string& name) : LocalFDB(config, name) {

    std::vector<std::string> stores;
    std::vector<std::string> fieldLocationEndpoints;

    client_ = std::make_shared<RemoteFDBClient>(config);
    client_->init(stores, fieldLocationEndpoints);

    eckit::Buffer buf2 = client_->controlWriteReadResponse(remote::Message::Schema, client_->generateRequestID());
    eckit::MemoryStream s2(buf2);

    fdb5::Schema* schema = eckit::Reanimator<fdb5::Schema>::reanimate(s2);

    config_.set("stores", stores);
    config_.set("fieldLocationEndpoints", fieldLocationEndpoints);
    config_.overrideSchema(static_cast<std::string>(client_->controlEndpoint()) + "/schema", schema);

    /// @note: We must instantiate the ReadLimiter before any RemoteStores due to their static initialisation.
    /// @todo: this may change in future.
    static size_t memoryLimit = eckit::Resource<size_t>(
        "$FDB_READ_LIMIT;fdbReadLimit",
        config_.userConfig().getUnsigned("limits.read", size_t(1) * 1024 * 1024 * 1024));  // 1GiB
    ReadLimiter::init(memoryLimit);
}

ListIterator RemoteFDB::list(const FDBToolRequest& request, const int depth) {
    return client_->forwardApiCall(ListHelper(depth), request);
}

AxesIterator RemoteFDB::axesIterator(const FDBToolRequest& request, const int depth) {
    return client_->forwardApiCall(AxesHelper(depth), request);
}

ListIterator RemoteFDB::inspect(const metkit::mars::MarsRequest& request) {
    return client_->forwardApiCall(InspectHelper(), request);
}

StatsIterator RemoteFDB::stats(const FDBToolRequest& request) {
    return client_->forwardApiCall(StatsHelper(), request);
}

void RemoteFDB::print(std::ostream& s) const {
    s << "RemoteFDB(...)";
}

static FDBBuilder<RemoteFDB> builder("remote");

}  // namespace fdb5

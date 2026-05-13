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
#include "fdb5/database/WipeState.h"

#include "fdb5/remote/RemoteFieldLocation.h"
#include "fdb5/remote/client/ClientConnectionRouter.h"
#include "fdb5/remote/client/ReadLimiter.h"

using namespace fdb5::remote;
using namespace eckit;
using namespace eckit::literals;

namespace {

using namespace fdb5;
template <typename T, Message msgID>
struct BaseAPIHelper {

    typedef T ValueType;

    static size_t bufferSize() { return 1_MiB; }
    static size_t queueSize() { return 100; }
    static Message message() { return msgID; }

    void encodeExtra(Stream& s) const {}
    static ValueType valueFromStream(Stream& s, RemoteFDB* fdb) { return ValueType(s); }
};

using StatsHelper = BaseAPIHelper<StatsElement, Message::Stats>;

struct ListHelper : BaseAPIHelper<ListElement, Message::List> {

    ListHelper(const int depth) : depth_(depth) {}

    static ListElement valueFromStream(Stream& s, RemoteFDB* fdb) {
        ListElement elem(s);
        std::shared_ptr<const FieldLocation> remoteLocation;

        if (elem.hasLocation()) {

            if (LibFdb5::instance().debug()) {
                Log::debug<LibFdb5>() << "ListHelper::valueFromStream - original location: ";
                elem.location().dump(Log::debug<LibFdb5>());
                Log::debug<LibFdb5>() << std::endl;
            }

            // TODO move the endpoint replacement to the server side ()
            if (elem.location().uri().scheme() == "fdb") {
                net::Endpoint fieldLocationEndpoint{elem.location().uri().host(), elem.location().uri().port()};

                remoteLocation = RemoteFieldLocation(fdb->storeEndpoint(fieldLocationEndpoint),
                                                     static_cast<const RemoteFieldLocation&>(elem.location()))
                                     .make_shared();
            }
            else {
                remoteLocation = RemoteFieldLocation(fdb->storeEndpoint(), elem.location()).make_shared();
            }
        }

        return ListElement(elem.keys(), remoteLocation, elem.timestamp());
    }

    void encodeExtra(Stream& s) const { s << depth_; }

private:

    int depth_{3};
};

struct AxesHelper : BaseAPIHelper<AxesElement, Message::Axes> {
    AxesHelper(int level) : level_(level) {}

    void encodeExtra(Stream& s) const { s << level_; }

private:

    int level_;
};

struct InspectHelper : BaseAPIHelper<ListElement, Message::Inspect> {

    static ListElement valueFromStream(Stream& s, RemoteFDB* fdb) {
        ListElement elem(s);

        if (LibFdb5::instance().debug()) {
            Log::debug<LibFdb5>() << "InspectHelper::valueFromStream - original location: ";
            elem.location().dump(Log::debug<LibFdb5>());
            Log::debug<LibFdb5>() << std::endl;
        }

        if (elem.location().uri().scheme() == "fdb") {
            net::Endpoint fieldLocationEndpoint{elem.location().uri().host(), elem.location().uri().port()};

            std::shared_ptr<const FieldLocation> remoteLocation =
                RemoteFieldLocation(fdb->storeEndpoint(fieldLocationEndpoint),
                                    static_cast<const RemoteFieldLocation&>(elem.location()))
                    .make_shared();
            return ListElement(elem.keys(), remoteLocation, elem.timestamp());
        }
        std::shared_ptr<const FieldLocation> remoteLocation =
            RemoteFieldLocation(fdb->storeEndpoint(), elem.location()).make_shared();
        return ListElement(elem.keys(), remoteLocation, elem.timestamp());
    }
};

struct WipeHelper : BaseAPIHelper<CatalogueWipeState, Message::Wipe> {

    WipeHelper(bool doit, bool porcelain, bool unsafeWipeAll) : doit_(doit), unsafeWipeAll_(unsafeWipeAll) {}

    void encodeExtra(Stream& s) const {
        s << doit_;
        s << unsafeWipeAll_;
    }

    static CatalogueWipeState valueFromStream(Stream& s, RemoteFDB* fdb) { return CatalogueWipeState(s); }

private:

    bool doit_;
    bool unsafeWipeAll_;
};

}  // namespace

namespace fdb5 {

const net::Endpoint& RemoteFDB::storeEndpoint() const {
    if (storesLocalFields_.empty()) {
        throw SeriousBug("Unable to find a store to serve local data");
    }
    return storesLocalFields_.at(std::rand() % storesLocalFields_.size());
}
const net::Endpoint& RemoteFDB::storeEndpoint(const net::Endpoint& fieldLocationEndpoint) const {
    // looking for an alias for the given endpoint
    auto it = storesReadMapping_.find(fieldLocationEndpoint);
    if (it == storesReadMapping_.end()) {
        std::ostringstream ss;
        ss << "Unable to find a matching endpoint. Looking for " << fieldLocationEndpoint << std::endl;
        ss << "Available endpoints:" << std::endl;
        for (auto s : storesReadMapping_) {
            ss << s.first << " --> " << s.second << std::endl;
        }
        throw SeriousBug(ss.str());
    }
    return it->second;
}

RemoteFDB::RemoteFDB(const Configuration& config, const std::string& name) : LocalFDB(config, name), Client(config) {

    Buffer buf = controlWriteReadResponse(remote::Message::Stores, generateRequestID());
    MemoryStream s(buf);
    size_t numStores;
    s >> numStores;
    ASSERT(numStores > 0);

    std::unordered_set<net::Endpoint> localFields;

    std::vector<std::string> stores;
    std::vector<std::string> fieldLocationEndpoints;

    for (size_t i = 0; i < numStores; i++) {
        std::string store;
        s >> store;
        size_t numAliases;
        s >> numAliases;
        std::vector<net::Endpoint> aliases;
        if (numAliases == 0) {
            net::Endpoint storeEndpoint{store};
            storesReadMapping_[storeEndpoint] = storeEndpoint;
            LOG_DEBUG_LIB(LibFdb5) << "store endpoint: " << storeEndpoint
                                   << " default data location endpoint: " << storeEndpoint << std::endl;
        }
        else {
            for (size_t j = 0; j < numAliases; j++) {
                net::Endpoint alias(s);
                if (store.empty()) {
                    storesLocalFields_.push_back(alias);
                    localFields.emplace(alias);
                }
                else {
                    net::Endpoint fieldLocationEndpoint{store};
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

    Buffer buf2 = controlWriteReadResponse(remote::Message::Schema, generateRequestID());
    MemoryStream s2(buf2);

    Schema* schema = Reanimator<Schema>::reanimate(s2);

    config_.set("stores", stores);
    config_.set("fieldLocationEndpoints", fieldLocationEndpoints);
    config_.overrideSchema(static_cast<std::string>(controlEndpoint()) + "/schema", schema);

    /// @note: We must instantiate the ReadLimiter before any RemoteStores due to their static initialisation.
    /// @todo: this may change in future.
    static size_t memoryLimit =
        Resource<size_t>("$FDB_READ_LIMIT;fdbReadLimit",
                         config_.userConfig().getUnsigned("limits.read", size_t(1) * 1024 * 1024 * 1024));  // 1GiB
    ReadLimiter::init(memoryLimit);
}

RemoteFDB::~RemoteFDB() {
    deregister();
}

// -----------------------------------------------------------------------------------------------------

// forwardApiCall captures the asynchronous behaviour:
//
// i) Set up a Queue to receive the messages as they come in
// ii) Encode the request+arguments and send them to the server
// iii) Return an AsyncIterator that pulls messages off the queue, and returns them to the caller.


template <typename HelperClass>
auto RemoteFDB::forwardApiCall(const HelperClass& helper, const FDBToolRequest& request)
    -> APIIterator<typename HelperClass::ValueType> {

    using ValueType = typename HelperClass::ValueType;
    using IteratorType = APIIterator<ValueType>;
    using AsyncIterator = APIAsyncIterator<ValueType>;

    // Reconnect if necessary
    refreshConnection();

    // Ensure we have an entry in the message queue before we trigger anything that
    // will result in return messages

    uint32_t id = generateRequestID();
    auto entry = messageQueues_.emplace(id, std::make_shared<MessageQueue>(HelperClass::queueSize()));
    ASSERT(entry.second);
    std::shared_ptr<MessageQueue> messageQueue(entry.first->second);

    // Encode the request and send it to the server

    Buffer encodeBuffer(HelperClass::bufferSize());
    MemoryStream s(encodeBuffer);
    s << request;
    helper.encodeExtra(s);

    controlWriteCheckResponse(HelperClass::message(), id, true, encodeBuffer, s.position());

    // Return an AsyncIterator to allow the messages to be retrieved in the API

    RemoteFDB* remoteFDB = this;
    return IteratorType(
        // n.b. Don't worry about catching exceptions in lambda, as
        // this is handled in the AsyncIterator.
        new AsyncIterator(shared_from_this(), [messageQueue, remoteFDB](Queue<ValueType>& queue) {
            Buffer msg{0};
            while (true) {
                if (messageQueue->pop(msg) == -1) {
                    break;
                }
                else {
                    MemoryStream s(msg);
                    queue.emplace(HelperClass::valueFromStream(s, remoteFDB));
                }
            }
            // messageQueue goes out of scope --> destructed
        }));
}

ListIterator RemoteFDB::list(const FDBToolRequest& request, const int depth) {
    return forwardApiCall(ListHelper(depth), request);
}

AxesIterator RemoteFDB::axesIterator(const FDBToolRequest& request, const int depth) {
    return forwardApiCall(AxesHelper(depth), request);
}

ListIterator RemoteFDB::inspect(const metkit::mars::MarsRequest& request) {
    return forwardApiCall(InspectHelper(), request);
}

StatsIterator RemoteFDB::stats(const FDBToolRequest& request) {
    return forwardApiCall(StatsHelper(), request);
}

WipeStateIterator RemoteFDB::wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) {
    return forwardApiCall(WipeHelper(doit, porcelain, unsafeWipeAll), request);
}

void RemoteFDB::print(std::ostream& s) const {
    s << "RemoteFDB(...)";
}

// Client
const Configuration& RemoteFDB::clientConfig() const {
    return config();
}

bool RemoteFDB::handle(remote::Message message, uint32_t requestID) {

    switch (message) {
        case Message::Complete: {

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
        case Message::Error: {

            std::ostringstream ss;
            ss << "RemoteFDB - client id: " << clientId()
               << " - received an error without error description for requestID " << requestID << std::endl;
            throw RemoteFDBException(ss.str(), controlEndpoint());

            return false;
        }
        default:
            return false;
    }
}
bool RemoteFDB::handle(remote::Message message, uint32_t requestID, Buffer&& payload) {

    switch (message) {
        case Message::Blob: {
            auto it = messageQueues_.find(requestID);
            if (it == messageQueues_.end()) {
                return false;
            }

            it->second->emplace(std::move(payload));
            return true;
        }

        case Message::Error: {

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

static FDBBuilder<RemoteFDB> builder("remote");

}  // namespace fdb5

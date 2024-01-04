#include <cstdlib>
#include <ctime>

#include "eckit/io/Buffer.h"
#include "eckit/log/Log.h"
#include "eckit/serialisation/MemoryStream.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "fdb5/api/RemoteFDB.h"
#include "fdb5/database/Archiver.h"
#include "fdb5/database/Inspector.h"
#include "fdb5/LibFdb5.h"

#include "fdb5/remote/client/ClientConnectionRouter.h"
#include "fdb5/remote/RemoteFieldLocation.h"

using namespace fdb5::remote;
using namespace eckit;

namespace {

template <typename T, fdb5::remote::Message msgID>
struct BaseAPIHelper {

    typedef T ValueType;

    static size_t bufferSize() { return 4096; }
    static size_t queueSize() { return 100; }
    static fdb5::remote::Message message() { return msgID; }

    void encodeExtra(eckit::Stream& s) const {}
    static ValueType valueFromStream(eckit::Stream& s, fdb5::RemoteFDB* fdb) { return ValueType(s); }
};

// using ListHelper = BaseAPIHelper<fdb5::ListElement, fdb5::remote::Message::List>;

struct ListHelper : BaseAPIHelper<fdb5::ListElement, fdb5::remote::Message::List> {

    static fdb5::ListElement valueFromStream(eckit::Stream& s, fdb5::RemoteFDB* fdb) {
        fdb5::ListElement elem(s);

        eckit::Log::debug<fdb5::LibFdb5>() << "ListHelper::valueFromStream - original location: ";
        elem.location().dump(eckit::Log::debug<fdb5::LibFdb5>());
        eckit::Log::debug<fdb5::LibFdb5>() << std::endl;

        if (elem.location().uri().scheme() == "fdb") {
            eckit::net::Endpoint endpoint{elem.location().uri().host(), elem.location().uri().port()};

            std::shared_ptr<fdb5::FieldLocation> remoteLocation = fdb5::remote::RemoteFieldLocation(fdb->storeEndpoint(endpoint), static_cast<const RemoteFieldLocation&>(elem.location())).make_shared();
            return fdb5::ListElement(elem.key(), remoteLocation, elem.timestamp());
        }
        std::shared_ptr<fdb5::FieldLocation> remoteLocation = fdb5::remote::RemoteFieldLocation(fdb->storeEndpoint(""), elem.location()).make_shared();
        return fdb5::ListElement(elem.key(), remoteLocation, elem.timestamp());
    }
};

struct InspectHelper : BaseAPIHelper<fdb5::ListElement, fdb5::remote::Message::Inspect> {

    static fdb5::ListElement valueFromStream(eckit::Stream& s, fdb5::RemoteFDB* fdb) {
        fdb5::ListElement elem(s);

        eckit::Log::debug<fdb5::LibFdb5>() << "InspectHelper::valueFromStream - original location: ";
        elem.location().dump(eckit::Log::debug<fdb5::LibFdb5>());
        eckit::Log::debug<fdb5::LibFdb5>() << std::endl;

        if (elem.location().uri().scheme() == "fdb") {
            eckit::net::Endpoint endpoint{elem.location().uri().host(), elem.location().uri().port()};

            std::shared_ptr<fdb5::FieldLocation> remoteLocation = fdb5::remote::RemoteFieldLocation(fdb->storeEndpoint(endpoint), static_cast<const RemoteFieldLocation&>(elem.location())).make_shared();
            return fdb5::ListElement(elem.key(), remoteLocation, elem.timestamp());
        }
        std::shared_ptr<fdb5::FieldLocation> remoteLocation = fdb5::remote::RemoteFieldLocation(fdb->storeEndpoint(""), elem.location()).make_shared();
        return fdb5::ListElement(elem.key(), remoteLocation, elem.timestamp());
    }
};
}

namespace fdb5 {

const eckit::net::Endpoint& RemoteFDB::storeEndpoint(const std::string& endpoint) const {
    auto it = stores_.find(endpoint);
    if (it == stores_.end()) {
        std::stringstream ss;
        ss << "Unable to find a matching endpoint. Looking for " << endpoint << std::endl;
        ss << "Available endpoints:";
        for (auto s : stores_) {
            ss << std::endl << s.first << " - ";
            for (auto e: s.second) {
                ss << e << ", ";
            }
        }
        throw eckit::SeriousBug(ss.str());
    }
    return it->second.at(std::rand() % it->second.size());
}

RemoteFDB::RemoteFDB(const eckit::Configuration& config, const std::string& name):
    LocalFDB(config, name),
    Client(eckit::net::Endpoint(config.getString("host"), config.getInt("port")), "") {

    uint32_t id = generateRequestID();
    eckit::Buffer buf = controlWriteReadResponse(remote::Message::Schema, id);
    eckit::MemoryStream s(buf);
    size_t numStores;
    s >> numStores;
    ASSERT(numStores > 0);

    std::vector<std::string> stores;
    std::vector<std::string> defaultEndpoints;

    for (size_t i=0; i<numStores; i++) {
        std::string store;
        s >> store;
        size_t numAliases;
        s >> numAliases;
        std::vector<eckit::net::Endpoint> aliases;
        if (numAliases == 0) {
            stores.push_back(store);
            defaultEndpoints.push_back(store);
            Log::debug<LibFdb5>() << "store endpoint: " << store << " default endpoint: " << store << std::endl;
        } else {
            for (size_t j=0; j<numAliases; j++) {
                eckit::net::Endpoint endpoint(s);
                aliases.push_back(endpoint);
                stores.push_back(endpoint);
                defaultEndpoints.push_back(store);
                Log::debug<LibFdb5>() << "store endpoint: " << endpoint << " default endpoint: " << store << std::endl;
            }
        }
        stores_.emplace(store, aliases);
    }

    config_.set("stores", stores);
    config_.set("storesDefaultEndpoints", defaultEndpoints);

    fdb5::Schema* schema = eckit::Reanimator<fdb5::Schema>::reanimate(s);

    // eckit::Log::debug<LibFdb5>() << *this << " - Received Store endpoint: " << storeEndpoint_ << " and master schema: " << std::endl;
    // schema->dump(eckit::Log::debug<LibFdb5>());
    
    config_.overrideSchema(static_cast<std::string>(controlEndpoint())+"/schema", schema);
}

// -----------------------------------------------------------------------------------------------------

// forwardApiCall captures the asynchronous behaviour:
//
// i) Set up a Queue to receive the messages as they come in
// ii) Encode the request+arguments and send them to the server
// iii) Return an AsyncIterator that pulls messages off the queue, and returns them to the caller.


template <typename HelperClass>
auto RemoteFDB::forwardApiCall(const HelperClass& helper, const FDBToolRequest& request) -> APIIterator<typename HelperClass::ValueType> {

    using ValueType = typename HelperClass::ValueType;
    using IteratorType = APIIterator<ValueType>;
    using AsyncIterator = APIAsyncIterator<ValueType>;

    // Ensure we have an entry in the message queue before we trigger anything that
    // will result in return messages

    uint32_t id = connection_.generateRequestID();
    auto entry = messageQueues_.emplace(id, std::make_shared<MessageQueue>(HelperClass::queueSize()));
    ASSERT(entry.second);
    std::shared_ptr<MessageQueue> messageQueue(entry.first->second);

    // Encode the request and send it to the server

    Buffer encodeBuffer(HelperClass::bufferSize());
    MemoryStream s(encodeBuffer);
    s << request;
    helper.encodeExtra(s);

    controlWriteCheckResponse(HelperClass::message(), id, encodeBuffer, s.position());

    // Return an AsyncIterator to allow the messages to be retrieved in the API

    RemoteFDB* remoteFDB = this;
    return IteratorType(
        // n.b. Don't worry about catching exceptions in lambda, as
        // this is handled in the AsyncIterator.
        new AsyncIterator([messageQueue, remoteFDB](eckit::Queue<ValueType>& queue) {
            eckit::Buffer msg{0};
                        while (true) {
                            if (messageQueue->pop(msg) == -1) {
                                break;
                            } else {
                                MemoryStream s(msg);
                                queue.emplace(HelperClass::valueFromStream(s, remoteFDB));
                            }
                        }
                        // messageQueue goes out of scope --> destructed
                    }
                )
           );
}

ListIterator RemoteFDB::list(const FDBToolRequest& request) {
    return forwardApiCall(ListHelper(), request);
}

ListIterator RemoteFDB::inspect(const metkit::mars::MarsRequest& request) {
    return forwardApiCall(InspectHelper(), request);
}

void RemoteFDB::print(std::ostream& s) const {
    s << "RemoteFDB(...)";
}

// Client
bool RemoteFDB::handle(remote::Message message, uint32_t requestID) {
    
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

            auto it = messageQueues_.find(requestID);
            if (it == messageQueues_.end()) {
                return false;
            }

            it->second->interrupt(std::make_exception_ptr(RemoteFDBException("", controlEndpoint())));
            // Remove entry (shared_ptr --> message queue will be destroyed when it
            // goes out of scope in the worker thread).
            messageQueues_.erase(it);
            return true;
        }
        default:
            return false;
    }
}
bool RemoteFDB::handle(remote::Message message, uint32_t requestID, eckit::Buffer&& payload) {

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
void RemoteFDB::handleException(std::exception_ptr e){NOTIMP;}

static FDBBuilder<RemoteFDB> builder("remote");

} // namespace fdb5

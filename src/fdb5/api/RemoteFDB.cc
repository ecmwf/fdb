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

using ListHelper = BaseAPIHelper<fdb5::ListElement, fdb5::remote::Message::List>;

struct InspectHelper : BaseAPIHelper<fdb5::ListElement, fdb5::remote::Message::Inspect> {

    static fdb5::ListElement valueFromStream(eckit::Stream& s, fdb5::RemoteFDB* fdb) {
        fdb5::ListElement elem(s);
        // std::cout << "InspectHelper::valueFromStream - ";
        // elem.location().dump(std::cout);
        // std::cout << std::endl;
        if (elem.location().uri().scheme() == "fdb") {
            return elem;
        }

        return fdb5::ListElement(elem.key(), fdb5::remote::RemoteFieldLocation(fdb->storeEndpoint(), elem.location()).make_shared(), elem.timestamp());
    }
};
}

namespace fdb5 {

eckit::net::Endpoint RemoteFDB::storeEndpoint() {
    ASSERT(localStores_.size() > 0);

    return localStores_.at(std::rand() % localStores_.size());
}

RemoteFDB::RemoteFDB(const eckit::Configuration& config, const std::string& name):
    LocalFDB(config, name),
    Client(eckit::net::Endpoint(config.getString("host"), config.getInt("port"))) {

    eckit::Buffer buf = controlWriteReadResponse(remote::Message::Schema);
    eckit::MemoryStream s(buf);
    size_t numStores;
    s >> numStores;
    std::vector<std::string> stores;
    for (size_t i=0; i<numStores; i++) {
        stores.push_back(eckit::net::Endpoint(s).hostport());
    }
    s >> numStores;
    std::vector<std::string> localStores;
    for (size_t i=0; i<numStores; i++) {
        eckit::net::Endpoint endpoint(s);
        localStores_.push_back(endpoint);
        localStores.push_back(endpoint.hostport());
    }

    // std::srand(std::time(nullptr));
    // eckit::net::Endpoint storeEndpoint = stores_.at(std::rand() % stores_.size());

    fdb5::Schema* schema = eckit::Reanimator<fdb5::Schema>::reanimate(s);

    // eckit::Log::debug<LibFdb5>() << *this << " - Received Store endpoint: " << storeEndpoint_ << " and master schema: " << std::endl;
    // schema->dump(eckit::Log::debug<LibFdb5>());
    
    config_.overrideSchema(endpoint_.hostport()+"/schema", schema);
    config_.set("stores", stores);
    config_.set("localStores", localStores);
    // config_.set("storeHost", storeEndpoint.host());
    // config_.set("storePort", storeEndpoint.port());
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


// ListIterator RemoteFDB::inspect(const metkit::mars::MarsRequest& request) {
//     doinspect_ = true;

//     // worker that does nothing but exposes the AsyncIterator's queue.
//     auto async_worker = [this] (Queue<ListElement>& queue) {
//         inspectqueue_ = &queue;
//         while(!queue.closed()) std::this_thread::sleep_for(std::chrono::milliseconds(10));
//     };

//     // construct iterator before contacting remote, because we want to point to the asynciterator's queue
//     auto iter = new APIAsyncIterator<ListElement>(async_worker); 

//     auto req = FDBToolRequest(request);
//     Buffer encodeBuffer(4096);
//     MemoryStream s(encodeBuffer);
//     s << req;
//     uint32_t id = controlWriteCheckResponse(Message::Inspect, encodeBuffer, s.position());

//     return APIIterator<ListElement>(iter);
// }


// ListIterator RemoteFDB::list(const FDBToolRequest& request) {

//     dolist_ = true;
    
//     // worker that does nothing but exposes the AsyncIterator's queue.
//     auto async_worker = [this] (Queue<ListElement>& queue) {
//         listqueue_ = &queue;
//         while(!queue.closed()) std::this_thread::sleep_for(std::chrono::milliseconds(10));
//     };

//     // construct iterator before contacting remote, because we want to point to the asynciterator's queue
//     auto iter = new APIAsyncIterator<ListElement>(async_worker); 

//     Buffer encodeBuffer(4096);
//     MemoryStream s(encodeBuffer);
//     s << request;

//     controlWriteCheckResponse(Message::List, encodeBuffer, s.position());
//     return APIIterator<ListElement>(iter);
// }

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

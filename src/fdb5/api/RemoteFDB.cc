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

    static size_t bufferSize() { return 1024*1024; }
    static size_t queueSize() { return 100; }
    static fdb5::remote::Message message() { return msgID; }

    void encodeExtra(eckit::Stream& s) const {}
    static ValueType valueFromStream(eckit::Stream& s, fdb5::RemoteFDB* fdb) { return ValueType(s); }
};

using StatsHelper = BaseAPIHelper<fdb5::StatsElement, fdb5::remote::Message::Stats>;

struct ListHelper : BaseAPIHelper<fdb5::ListElement, fdb5::remote::Message::List> {

    static fdb5::ListElement valueFromStream(eckit::Stream& s, fdb5::RemoteFDB* fdb) {
        fdb5::ListElement elem(s);

        eckit::Log::debug<fdb5::LibFdb5>() << "ListHelper::valueFromStream - original location: ";
        elem.location().dump(eckit::Log::debug<fdb5::LibFdb5>());
        eckit::Log::debug<fdb5::LibFdb5>() << std::endl;

        // TODO move the endpoint replacement to the server side ()
        if (elem.location().uri().scheme() == "fdb") {
            eckit::net::Endpoint fieldLocationEndpoint{elem.location().uri().host(), elem.location().uri().port()};

            std::shared_ptr<const fdb5::FieldLocation> remoteLocation = fdb5::remote::RemoteFieldLocation(fdb->storeEndpoint(fieldLocationEndpoint), static_cast<const RemoteFieldLocation&>(elem.location())).make_shared();
            return fdb5::ListElement(elem.key(), remoteLocation, elem.timestamp());
        }
        std::shared_ptr<const fdb5::FieldLocation> remoteLocation = fdb5::remote::RemoteFieldLocation(fdb->storeEndpoint(), elem.location()).make_shared();
        return fdb5::ListElement(elem.key(), remoteLocation, elem.timestamp());
    }
};

struct AxesHelper : BaseAPIHelper<fdb5::AxesElement, fdb5::remote::Message::Axes> {
    AxesHelper(int level) : level_(level) {}

    void encodeExtra(eckit::Stream& s) const {
        s << level_;
    }
private:
    int level_;
};

struct InspectHelper : BaseAPIHelper<fdb5::ListElement, fdb5::remote::Message::Inspect> {

    static fdb5::ListElement valueFromStream(eckit::Stream& s, fdb5::RemoteFDB* fdb) {
        fdb5::ListElement elem(s);

        eckit::Log::debug<fdb5::LibFdb5>() << "InspectHelper::valueFromStream - original location: ";
        elem.location().dump(eckit::Log::debug<fdb5::LibFdb5>());
        eckit::Log::debug<fdb5::LibFdb5>() << std::endl;

        if (elem.location().uri().scheme() == "fdb") {
            eckit::net::Endpoint fieldLocationEndpoint{elem.location().uri().host(), elem.location().uri().port()};

            std::shared_ptr<const fdb5::FieldLocation> remoteLocation = fdb5::remote::RemoteFieldLocation(fdb->storeEndpoint(fieldLocationEndpoint), static_cast<const RemoteFieldLocation&>(elem.location())).make_shared();
            return fdb5::ListElement(elem.key(), remoteLocation, elem.timestamp());
        }
        std::shared_ptr<const fdb5::FieldLocation> remoteLocation = fdb5::remote::RemoteFieldLocation(fdb->storeEndpoint(), elem.location()).make_shared();
        return fdb5::ListElement(elem.key(), remoteLocation, elem.timestamp());
    }
};

struct WipeHelper : BaseAPIHelper<fdb5::WipeElement, fdb5::remote::Message::Wipe> {

    WipeHelper(bool doit, bool porcelain, bool unsafeWipeAll) :
        doit_(doit), porcelain_(porcelain), unsafeWipeAll_(unsafeWipeAll) {}

    void encodeExtra(eckit::Stream& s) const {
        s << doit_;
        s << porcelain_;
        s << unsafeWipeAll_;
    }

    static fdb5::WipeElement valueFromStream(eckit::Stream& s, fdb5::RemoteFDB* fdb) {
        fdb5::WipeElement elem{s};
        return elem;
    }

private:
    bool doit_;
    bool porcelain_;
    bool unsafeWipeAll_;
};

}

namespace fdb5 {

const eckit::net::Endpoint& RemoteFDB::storeEndpoint() const {
    if (storesLocalFields_.empty()) {
        throw eckit::SeriousBug("Unable to find a store to serve local data");
    }
    return storesLocalFields_.at(std::rand() % storesLocalFields_.size());
}
const eckit::net::Endpoint& RemoteFDB::storeEndpoint(const eckit::net::Endpoint& fieldLocationEndpoint) const {
    // looking for an alias for the given endpoint
    auto it = storesReadMapping_.find(fieldLocationEndpoint);
    if (it == storesReadMapping_.end()) {
        std::stringstream ss;
        ss << "Unable to find a matching endpoint. Looking for " << fieldLocationEndpoint << std::endl;
        ss << "Available endpoints:" << std::endl;
        for (auto s : storesReadMapping_) {
            ss << s.first << " --> " << s.second << std::endl;
        }
        throw eckit::SeriousBug(ss.str());
    }
    return it->second;
}

RemoteFDB::RemoteFDB(const eckit::Configuration& config, const std::string& name):
    LocalFDB(config, name),
    Client(eckit::net::Endpoint(config.getString("host"), config.getInt("port")), "") {

    eckit::Buffer buf = controlWriteReadResponse(remote::Message::Stores, generateRequestID());
    eckit::MemoryStream s(buf);
    size_t numStores;
    s >> numStores;
    ASSERT(numStores > 0);

    std::unordered_set<eckit::net::Endpoint> localFields;

    std::vector<std::string> stores;
    std::vector<std::string> fieldLocationEndpoints;
    
    for (size_t i=0; i<numStores; i++) {
        std::string store;
        s >> store;
        size_t numAliases;
        s >> numAliases;
        std::vector<eckit::net::Endpoint> aliases;
        if (numAliases == 0) {
            eckit::net::Endpoint storeEndpoint{store};
            storesReadMapping_[storeEndpoint] = storeEndpoint;
            LOG_DEBUG_LIB(LibFdb5) << "store endpoint: " << storeEndpoint << " default data location endpoint: " << storeEndpoint << std::endl;
        } else {
            for (size_t j=0; j<numAliases; j++) {
                eckit::net::Endpoint alias(s);
                if (store.empty()) {
                    storesLocalFields_.push_back(alias);
                    localFields.emplace(alias);
                } else {
                    eckit::net::Endpoint fieldLocationEndpoint{store};
                    storesReadMapping_[fieldLocationEndpoint] = alias;
                }
                LOG_DEBUG_LIB(LibFdb5) << "store endpoint: " << alias << " default data location endpoint: " << store << std::endl;
            }
        }
    }
    for(const auto& s: storesReadMapping_) {
        if (localFields.find(s.second) == localFields.end()) {
            storesArchiveMapping_.push_back(std::make_pair(s.second, s.first));
            stores.push_back(s.second);
            fieldLocationEndpoints.push_back(s.first);
        }
    }
    for (const auto& s: storesLocalFields_) {
        stores.push_back(s);
        fieldLocationEndpoints.push_back("");
    }

    eckit::Buffer buf2 = controlWriteReadResponse(remote::Message::Schema, generateRequestID());
    eckit::MemoryStream s2(buf2);

    fdb5::Schema* schema = eckit::Reanimator<fdb5::Schema>::reanimate(s2);

    config_.set("stores", stores);
    config_.set("fieldLocationEndpoints", fieldLocationEndpoints);
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
            })
        );
}

ListIterator RemoteFDB::list(const FDBToolRequest& request) {
    return forwardApiCall(ListHelper(), request);
}

AxesIterator RemoteFDB::axesIterator(const FDBToolRequest& request, int level) {
    return forwardApiCall(AxesHelper(level), request);
}

ListIterator RemoteFDB::inspect(const metkit::mars::MarsRequest& request) {
    return forwardApiCall(InspectHelper(), request);
}

StatsIterator RemoteFDB::stats(const FDBToolRequest& request) {
    return forwardApiCall(StatsHelper(), request);
}

WipeIterator RemoteFDB::wipe(const FDBToolRequest& request, bool doit, bool porcelain, bool unsafeWipeAll) {
    return forwardApiCall(WipeHelper(doit, porcelain, unsafeWipeAll), request);
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

static FDBBuilder<RemoteFDB> builder("remote");

} // namespace fdb5

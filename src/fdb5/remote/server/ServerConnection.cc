/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include <chrono>

#include "eckit/config/Resource.h"
#include "eckit/maths/Functions.h"
#include "eckit/net/Endpoint.h"
#include "eckit/runtime/Main.h"
#include "eckit/runtime/SessionID.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/fdb5_version.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/database/Key.h"
#include "fdb5/remote/AvailablePortList.h"
#include "fdb5/remote/Messages.h"
#include "fdb5/remote/RemoteFieldLocation.h"
#include "fdb5/api/FDB.h"

#include "fdb5/remote/server/ServerConnection.h"

namespace fdb5::remote {

// helpers
namespace {

class TCPException : public eckit::Exception {
public:
    TCPException(const std::string& msg, const eckit::CodeLocation& here) :
        eckit::Exception(std::string("TCPException: ") + msg, here) {}
};

std::vector<int> intersection(const eckit::LocalConfiguration& c1, const eckit::LocalConfiguration& c2, const std::string& field){

    std::vector<int> v1 = c1.getIntVector(field);
    std::vector<int> v2 = c2.getIntVector(field);
    std::vector<int> v3;

    std::sort(v1.begin(), v1.end());
    std::sort(v2.begin(), v2.end());

    std::set_intersection(v1.begin(),v1.end(),
                          v2.begin(),v2.end(),
                          back_inserter(v3));
    return v3;
}

} // namespace


//size_t ServerConnection::queueSize_ = eckit::Resource<size_t>("fdbServerMaxQueueSize", 32);

ServerConnection::ServerConnection(eckit::net::TCPSocket& socket, const Config& config) :
        Connection(), config_(config),
        dataListenHostname_(config.getString("dataListenHostname", "")),
        readLocationQueue_(eckit::Resource<size_t>("fdbRetrieveQueueSize", 10000)), 
        archiveQueue_(eckit::Resource<size_t>("fdbServerMaxQueueSize", 32)),
        controlSocket_(socket), dataSocket_(nullptr), dataListener_(0) {

    eckit::Log::debug<LibFdb5>() << "ServerConnection::ServerConnection initialized" << std::endl;
}

ServerConnection::~ServerConnection() {
    // We don't want to die before the worker threads are cleaned up
    waitForWorkers();

    // And notify the client that we are done.
//     eckit::Log::info() << "Sending exit message to client" << std::endl;
// //    write(Message::Exit, true, 0, 0);
//     write(Message::Exit, false, 0, 0);
    eckit::Log::info() << "Done" << std::endl;
}


Handled ServerConnection::handleData(Message message, uint32_t clientID, uint32_t requestID) {
    try {
        switch (message) {
            case Message::Flush: // notification that the client has sent all data for archival
                return Handled::YesRemoveArchiveListener; // ????

            default: {
                std::stringstream ss;
                ss << "ERROR: Unexpected message recieved (" << message << "). ABORTING";
                eckit::Log::status() << ss.str() << std::endl;
                eckit::Log::error() << ss.str() << std::endl;
                throw eckit::SeriousBug(ss.str(), Here());
            }
        }
    }
    catch (std::exception& e) {
        // n.b. more general than eckit::Exception
        error(e.what(), clientID, requestID);
    }
    catch (...) {
        error("Caught unexpected and unknown error", clientID, requestID);
    }
    return Handled::No;
}

Handled ServerConnection::handleData(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {
    try {
        switch (message) {
            case Message::Blob:
            case Message::MultiBlob:
                queue(message, clientID, requestID, std::move(payload));
                return Handled::Yes;

            default: {
                std::stringstream ss;
                ss << "ERROR: Unexpected message recieved (" << message << "). ABORTING";
                eckit::Log::status() << ss.str() << std::endl;
                eckit::Log::error() << ss.str() << std::endl;
                throw eckit::SeriousBug(ss.str(), Here());
            }
        }
    }
    catch (std::exception& e) {
        // n.b. more general than eckit::Exception
        error(e.what(), clientID, requestID);
    }
    catch (...) {
        error("Caught unexpected and unknown error", clientID, requestID);
    }
    return Handled::No;
}

// ServerConnection::ServerConnection(eckit::net::TCPSocket& socket, const Config& config) :
//         config_(config),
//         controlSocket_(socket),
//         dataSocket_(selectDataPort()),
//         dataListenHostname_(config.getString("dataListenHostname", "")),
//         readLocationQueue_(eckit::Resource<size_t>("fdbRetrieveQueueSize", 10000)) {
//             eckit::Log::debug<LibFdb5>() << "ServerConnection::ServerConnection initialized" << std::endl;
//     }

// ServerConnection::~ServerConnection() {
//     // We don't want to die before the worker threads are cleaned up
//     waitForWorkers();

//     // And notify the client that we are done.
//     eckit::Log::info() << "Sending exit message to client" << std::endl;
//     dataWrite(Message::Exit, 0, 0);
//     eckit::Log::info() << "Done" << std::endl;
// }

eckit::LocalConfiguration ServerConnection::availableFunctionality() const {
    eckit::LocalConfiguration conf;
//    Add to the configuration all the components that require to be versioned, as in the following example, with a vector of supported version numbers
    std::vector<int> remoteFieldLocationVersions = {1};
    conf.set("RemoteFieldLocation", remoteFieldLocationVersions);
    std::vector<int> numberOfConnections = {2};
    conf.set("NumberOfConnections", numberOfConnections);
    return conf;
}


void ServerConnection::initialiseConnections() {
    // Read the startup message from the client. Check that it all checks out.

    MessageHeader hdr;
    eckit::Buffer payload1 = readControl(hdr);

    ASSERT(hdr.message == Message::Startup);
    ASSERT(hdr.requestID == 0);

    eckit::MemoryStream s1(payload1);
    eckit::SessionID clientSession(s1);
    eckit::net::Endpoint endpointFromClient(s1);
    unsigned int remoteProtocolVersion = 0;
    std::string errorMsg;

    try {
        s1 >> remoteProtocolVersion;
    } catch (...) {
        errorMsg = "Error retrieving client protocol version";
    }

    if (errorMsg.empty() && !LibFdb5::instance().remoteProtocolVersion().check(remoteProtocolVersion, false)) {
        std::stringstream ss;
        ss << "FDB server version " << fdb5_version_str() << " - remote protocol version not supported:" << std::endl;
        ss << "    versions supported by server: " << LibFdb5::instance().remoteProtocolVersion().supportedStr() << std::endl;
        ss << "    version requested by client: " << remoteProtocolVersion << std::endl;
        errorMsg = ss.str();
    }

    if (errorMsg.empty()) {
        eckit::LocalConfiguration clientAvailableFunctionality(s1);
        eckit::LocalConfiguration serverConf = availableFunctionality();
        agreedConf_ = eckit::LocalConfiguration();
        bool compatibleProtocol = true;

        
        std::vector<int> rflCommon = intersection(clientAvailableFunctionality, serverConf, "RemoteFieldLocation");
        if (rflCommon.size() > 0) {
            eckit::Log::debug<LibFdb5>() << "Protocol negotiation - RemoteFieldLocation version " << rflCommon.back() << std::endl;
            agreedConf_.set("RemoteFieldLocation", rflCommon.back());
        } else {
            compatibleProtocol = false;
        }

        if (!clientAvailableFunctionality.has("NumberOfConnections")) { // set the default
            std::vector<int> conn = {2};
            clientAvailableFunctionality.set("NumberOfConnections", conn);
        }
        // agree on a common functionality by intersecting server and client version numbers
        std::vector<int> ncCommon = intersection(clientAvailableFunctionality, serverConf, "NumberOfConnections");
        if (ncCommon.size() > 0) {
            int ncSelected = 2;

            if (ncCommon.size() == 1) {
                ncSelected = ncCommon.at(0);
            } else {
                ncSelected = ncCommon.back();
                if (clientAvailableFunctionality.has("PreferSingleConnection")) {
                    if ( std::find(ncCommon.begin(), ncCommon.end(), (clientAvailableFunctionality.getBool("PreferSingleConnection") ? 1 : 2)) != ncCommon.end() ) {
                        ncSelected = (clientAvailableFunctionality.getBool("PreferSingleConnection") ? 1 : 2);
                    }
                }
            }

            eckit::Log::debug<LibFdb5>() << "Protocol negotiation - NumberOfConnections " << ncSelected << std::endl;
            agreedConf_.set("NumberOfConnections", ncSelected);
            single_ = (ncSelected == 1);
        }
        else {
            std::stringstream ss;
            ss << "FDB server version " << fdb5_version_str() << " - failed protocol negotiation with FDB client" << std::endl;
            ss << "    server functionality: " << serverConf << std::endl;
            ss << "    client functionality: " << clientAvailableFunctionality << std::endl;
            errorMsg = ss.str();
        }
    }

    // We want a data connection too. Send info to RemoteFDB, and wait for connection
    // n.b. FDB-192: we use the host communicated from the client endpoint. This
    //               ensures that if a specific interface has been selected and the
    //               server has multiple, then we use that on, whilst retaining
    //               the capacity in the protocol for the server to make a choice.

    eckit::net::Endpoint dataEndpoint;
    if (single_) {
        dataEndpoint = endpointFromClient;
    } else {
        dataSocket_.reset(new eckit::net::EphemeralTCPServer(selectDataPort()));
        dataEndpoint = eckit::net::Endpoint{endpointFromClient.hostname(), dataSocket_->localPort()};
    }

    eckit::Log::info() << "Sending data endpoint to client: " << dataEndpoint << std::endl;
    {
        eckit::Buffer startupBuffer(1024);
        eckit::MemoryStream s(startupBuffer);

        s << clientSession;
        s << sessionID_;
        s << dataEndpoint;
        s << agreedConf_.get();
        // s << storeEndpoint; // xxx single-store case only: we cant do this with multiple stores // For now, dont send the store endpoint to the client 

        eckit::Log::debug<LibFdb5>() << "Protocol negotiation - configuration: " << agreedConf_ <<std::endl;

        write(Message::Startup, true, 0, 0, std::vector<std::pair<const void*, uint32_t>>{{startupBuffer.data(), s.position()}});
    }


    if (!errorMsg.empty()) {
        error(errorMsg, hdr.clientID(), hdr.requestID);
        return;
    }

    if (!single_) {
        dataSocket_->accept();

        // Check the response from the client.
        // Ensure that the hostname matches the original hostname, and that
        // it returns the details we sent it
        // IE check that we are connected to the correct client!

        MessageHeader dataHdr;
        eckit::Buffer payload2 = readData(dataHdr);

        ASSERT(dataHdr.version == CurrentVersion);
        ASSERT(dataHdr.message == Message::Startup);
        ASSERT(dataHdr.requestID == 0);

        eckit::MemoryStream s2(payload2);
        eckit::SessionID clientSession2(s2);
        eckit::SessionID serverSession(s2);

        if (clientSession != clientSession2) {
            std::stringstream ss;
            ss << "Client session IDs do not match: " << clientSession << " != " << clientSession2;
            throw eckit::BadValue(ss.str(), Here());
        }

        if (serverSession != sessionID_) {
            std::stringstream ss;
            ss << "Session IDs do not match: " << serverSession << " != " << sessionID_;
            throw eckit::BadValue(ss.str(), Here());
        }
    }
}

int ServerConnection::selectDataPort() {
    eckit::Log::info() << "SelectDataPort: " << std::endl;
    eckit::Log::info() << config_ << std::endl;
    if (config_.has("dataPortStart")) {
        ASSERT(config_.has("dataPortCount"));
        return AvailablePortList(config_.getInt("dataPortStart"), config_.getLong("dataPortCount"))
            .acquire();
    }

    // Use a system assigned port
    return 0;
}

size_t ServerConnection::archiveThreadLoop() {
    size_t totalArchived = 0;

    ArchiveElem elem;

    try {
        long queuelen;
        while ((queuelen = archiveQueue_.pop(elem)) != -1) {
            if (elem.multiblob_) {
                // Handle MultiBlob

                const char* firstData = static_cast<const char*>(elem.payload_.data());  // For pointer arithmetic
                const char* charData = firstData;
                while (size_t(charData - firstData) < elem.payload_.size()) {
                    const MessageHeader* hdr = static_cast<const MessageHeader*>(static_cast<const void*>(charData));
                    ASSERT(hdr->message == Message::Blob);
                    ASSERT(hdr->clientID() == elem.clientID_);
                    ASSERT(hdr->requestID == elem.requestID_);
                    charData += sizeof(MessageHeader);

                    const void* payloadData = charData;
                    charData += hdr->payloadSize;

                    const decltype(EndMarker)* e = static_cast<const decltype(EndMarker)*>(static_cast<const void*>(charData));
                    ASSERT(*e == EndMarker);
                    charData += sizeof(EndMarker);

                    archiveBlob(elem.clientID_, elem.requestID_, payloadData, hdr->payloadSize);
                    totalArchived += 1;
                }
            }
            else {
                // Handle single blob
                archiveBlob(elem.clientID_, elem.requestID_, elem.payload_.data(), elem.payload_.size());
                totalArchived += 1;
            }
        }

        // eckit::Buffer buffer(1024);
        // eckit::MemoryStream stream(buffer);
        // stream << totalArchived;
        
        // write(Message::Complete, false, 0, 0, buffer, stream.position());
    }
    catch (...) {
        // Ensure exception propagates across the queue back to the parent thread.
        archiveQueue_.interrupt(std::current_exception());
        throw;
    }

    return totalArchived;
}



void ServerConnection::listeningThreadLoopData() {

    MessageHeader hdr;
    uint32_t archiverID;

    try {

        // The archive loop is the only thing that can listen on the data socket,
        // so we don't need to to anything clever here.

        // n.b. we also don't need to lock on read. We are the only thing that reads.
        while (true) {
            eckit::Buffer payload = readData(hdr); // READ DATA
            // eckit::Log::debug<LibFdb5>() << "ServerConnection::listeningThreadLoopData - got [message=" << hdr.message << ",clientID="<< hdr.clientID() << ",requestID=" << hdr.requestID << ",payload=" << hdr.payloadSize << "]" << std::endl;

        if (hdr.message == Message::Exit) {
            if (remove(hdr.clientID())) {
                eckit::Log::status() << "Exiting" << std::endl;
                eckit::Log::info() << "Exiting" << std::endl;

                return;
            }
        } else {
            
            Handled handled;
            if (payload.size() == 0) {
                handled = handleData(hdr.message, hdr.clientID(), hdr.requestID);
            } else {
                handled = handleData(hdr.message, hdr.clientID(), hdr.requestID, std::move(payload));
            }


        switch (handled)
        {
            case Handled::Replied: // nothing to do
                break;
            case Handled::YesRemoveArchiveListener:
                dataListener_--;
                if (dataListener_ == 0) {
                    return;
                    // listeningThreadData.join();
                }
                break;
            case Handled::YesRemoveReadListener:
                dataListener_--;
                if (dataListener_ == 0) {
                    return;
                    // listeningThreadData.join();
                }
                break;
            case Handled::Yes:
//                write(Message::Received, false, hdr.clientID(), hdr.requestID);
                break;
            case Handled::No:
            default:
                std::stringstream ss;
                ss << "Unable to handle message " << hdr.message << " received on the data connection";
                error(ss.str(), hdr.clientID(), hdr.requestID);
        }
        }

            // std::cout << "listeningThreadLoopData " << hdr.message << " " << hdr.clientID() << " " << hdr.requestID << " " << ((int) handled) << std::endl;
            // switch (handled)
            // {
            //     case Handled::YesRemoveArchiveListener:
            //         dataListener_--;
            //         if (dataListener_ == 0) {
            //             // queue.close();
            //             return;
            //         }
            //         break;
            //     case Handled::Yes:
            //         break;
            //     case Handled::Replied:
            //     case Handled::No:
            //     default:
            //         std::stringstream ss;
            //         ss << "Unable to handle message " << hdr.message << " from data connection";
            //         error(ss.str(), hdr.clientID(), hdr.requestID);
            // }
        }

        // // Trigger cleanup of the workers
        // auto q = archiveQueues_.find(archiverID);
        // ASSERT(q != archiveQueues_.end());
        // q->second.close();

        // auto w = archiveFuture_.find(archiverID);
        // ASSERT(w != archiveFuture_.end());
        // // Ensure worker is done
        // ASSERT(w->second.valid());
        // totalArchived = worker.get();  // n.b. use of async, get() propagates any exceptions.
    }
    catch (std::exception& e) {
        // n.b. more general than eckit::Exception
        error(e.what(), hdr.clientID(), hdr.requestID);
        // auto q = archiveQueues_.find(archiverID);
        // if(q != archiveQueues_.end()) {
        //     q->second.interrupt(std::current_exception());
        // }
        throw;
    }
    catch (...) {
        error("Caught unexpected, unknown exception in retrieve worker", hdr.clientID(), hdr.requestID);
        // auto q = archiveQueues_.find(archiverID);
        // if(q != archiveQueues_.end()) {
        //     q->second.interrupt(std::current_exception());
        // }
        throw;
    }
}

void ServerConnection::handle() {
    initialiseConnections();
 
    std::thread listeningThreadData;
    // if (!single_) {
    //     listeningThreadData = std::thread([this] { listeningThreadLoopData(); });
    // }

    MessageHeader hdr;

    // listen loop
    while (true) {
        //tidyWorkers();

        eckit::Buffer payload = readControl(hdr); // READ CONTROL
        eckit::Log::debug<LibFdb5>() << "ServerConnection::handle - got [message=" << hdr.message << ",clientID="<< hdr.clientID() << ",requestID=" << hdr.requestID << ",payload=" << hdr.payloadSize << "]" << std::endl;

        if (hdr.message == Message::Exit) {
            // write(Message::Exit, true, hdr.clientID(), 0);
            eckit::Log::info() << "Exit " << controlSocket().localPort() << "  client " <<  hdr.clientID() << std::endl;
            write(Message::Exit, false, hdr.clientID(), 0);

            if (remove(hdr.clientID())) {
                eckit::Log::status() << "Exiting" << std::endl;
                eckit::Log::info() << "Exiting" << std::endl;

                return;
            }
        } else {

            Handled handled = Handled::No;
            ASSERT(single_ || hdr.control());

            if (payload.size()) {
                if (hdr.control()) {
                    handled = handleControl(hdr.message, hdr.clientID(), hdr.requestID, std::move(payload));
                } else {
                    handled = handleData(hdr.message, hdr.clientID(), hdr.requestID, std::move(payload));
                }
            } else {
                if (hdr.control()) {
                    handled = handleControl(hdr.message, hdr.clientID(), hdr.requestID);
                } else {
                    handled = handleData(hdr.message, hdr.clientID(), hdr.requestID);
                }
            }
            

            switch (handled)
            {
                case Handled::Replied: // nothing to do
                    break;
                // case Handled::YesRemoveArchiveListener:
                //     dataListener_--;
                //     if (dataListener_ == 0) {
                //         //return;
                //         // listeningThreadData.join();
                //     }
                //     break;
                case Handled::YesAddArchiveListener:
                    dataListener_++;
                    if (dataListener_ == 1) {
                        listeningThreadData = std::thread([this] { listeningThreadLoopData(); });
                    }
                    write(Message::Received, false, hdr.clientID(), hdr.requestID);
                    break;
                // case Handled::YesRemoveReadListener:
                //     dataListener_--;
                //     if (dataListener_ == 0) {
                //         //return;
                //         // listeningThreadData.join();
                //     }
                //     break;
                case Handled::YesAddReadListener:
                    dataListener_++;
                    if (dataListener_ == 1) {
                        listeningThreadData = std::thread([this] { listeningThreadLoopData(); });
                    }
                    write(Message::Received, false, hdr.clientID(), hdr.requestID);
                    break;
                case Handled::Yes:
                    write(Message::Received, false, hdr.clientID(), hdr.requestID);
                    break;
                case Handled::No:
                default:
                    std::stringstream ss;
                    ss << "Unable to handle message " << hdr.message;
                    error(ss.str(), hdr.clientID(), hdr.requestID);
            }
        }
    }
}

void ServerConnection::handleException(std::exception_ptr e) {
    try
    {
        if (e)
            std::rethrow_exception(e);
    }
    catch(const std::exception& e)
    {
        error(e.what(), 0, 0);
    }
}

void ServerConnection::queue(Message message, uint32_t clientID, uint32_t requestID, eckit::Buffer&& payload) {

    archiveQueue_.emplace(
        ArchiveElem{clientID, requestID, std::move(payload), message == Message::MultiBlob});
}

void ServerConnection::tidyWorkers() {
    std::map<uint32_t, std::future<void>>::iterator it = workerThreads_.begin();

    for (; it != workerThreads_.end(); /* empty */) {
        std::future_status stat = it->second.wait_for(std::chrono::milliseconds(0));

        if (stat == std::future_status::ready) {
            eckit::Log::info() << "Tidying up worker for request ID: " << it->first << std::endl;
            workerThreads_.erase(it++);
        }
        else {
            ++it;
        }
    }
}


void ServerConnection::archiver() {

    // Ensure that we aren't already running a catalogue/store
    if (!archiveFuture_.valid()) {
        // Start archive worker thread
        archiveFuture_ = std::async(std::launch::async, [this] { return archiveThreadLoop(); });
    }

    // // Start data reader thread if double connection and we aren't already running it
    // if (!single_ &&  !dataReader_.valid()) {
    //     dataReader_ = std::async(std::launch::async, [this] { return listeningThreadLoopData(); });
    // }
}

void ServerConnection::waitForWorkers() {
    readLocationQueue_.close();

    tidyWorkers();

    for (auto& it : workerThreads_) {
        eckit::Log::error() << "Worker thread still alive for request ID: " << it.first << std::endl;
        eckit::Log::error() << "Joining ..." << std::endl;
        it.second.get();
        eckit::Log::error() << "Thread complete" << std::endl;
    }

    if (readLocationWorker_.joinable()) {
        readLocationWorker_.join();
    }
}

}  // namespace fdb5::remote

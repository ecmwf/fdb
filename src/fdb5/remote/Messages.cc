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

#include "fdb5/remote/Messages.h"

namespace fdb5::remote {

//----------------------------------------------------------------------------------------------------------------------

std::ostream& operator<<(std::ostream& s, const Message& m) {
    switch (m) {
        case Message::None:
            s << "None";
            break;
        case Message::Exit:
            s << "Exit";
            break;
        case Message::Startup:
            s << "Startup";
            break;
        case Message::Error:
            s << "Error";
            break;
        case Message::Stop:
            s << "Stop";
            break;
        case Message::Stores:
            s << "Stores";
            break;
        case Message::Schema:
            s << "Schema";
            break;

            // API calls to forward
        case Message::Flush:
            s << "Flush";
            break;
        case Message::Archive:
            s << "Archive";
            break;
        case Message::Retrieve:
            s << "Retrieve";
            break;
        case Message::List:
            s << "List";
            break;
        case Message::Dump:
            s << "Dump";
            break;
        case Message::Status:
            s << "Status";
            break;
        case Message::Wipe:
            s << "Wipe";
            break;
        case Message::Purge:
            s << "Purge";
            break;
        case Message::Stats:
            s << "Stats";
            break;
        case Message::Control:
            s << "Control";
            break;
        case Message::Inspect:
            s << "Inspect";
            break;
        case Message::Read:
            s << "Read";
            break;
        case Message::Move:
            s << "Move";
            break;
        case Message::Store:
            s << "Store";
            break;
        case Message::Axes:
            s << "Axes";
            break;
        case Message::Exists:
            s << "Exists";
            break;
        case Message::DoWipeURIs:
            s << "DoWipeURIs";
            break;
        case Message::DoWipeUnknowns:
            s << "DoWipeUnknowns";
            break;
        case Message::DoWipeFinish:
            s << "DoWipeFinish";
            break;
        case Message::DoMaskIndexEntries:
            s << "DoMaskIndexEntries";
            break;
        case Message::DoUnsafeFullWipe:
            s << "DoUnsafeFullWipe";
            break;

            // Responses
        case Message::Received:
            s << "Received";
            break;
        case Message::Complete:
            s << "Complete";
            break;

            // Data communication
        case Message::Blob:
            s << "Blob";
            break;
        case Message::MultiBlob:
            s << "MultiBlob";
            break;
    }
    s << "(" << ((int)m) << ")";
    return s;
}

MessageHeader::MessageHeader(Message message, bool control, uint32_t clientID, uint32_t requestID,
                             uint32_t payloadSize) :
    marker(StartMarker),
    version(currentVersion),
    message(message),
    clientID_((clientID << 1) + (control ? 1 : 0)),
    requestID(requestID),
    payloadSize(payloadSize) {}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5::remote

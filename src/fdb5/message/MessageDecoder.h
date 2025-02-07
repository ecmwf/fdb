/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   MessageDecoder.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_MessageDecoder_H
#define fdb5_MessageDecoder_H


#include <vector>
#include "eckit/io/Buffer.h"
#include "metkit/mars/MarsRequest.h"

struct grib_handle;

namespace eckit {
class PathName;
}

namespace metkit {
namespace data {
class Reader;
class Message;
}  // namespace data
}  // namespace metkit

#include "fdb5/database/Key.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


class MessageDecoder {
public:

    MessageDecoder(bool checkDuplicates = false);

    virtual ~MessageDecoder();

    static Key messageToKey(const eckit::message::Message& msg);
    void messageToKey(const eckit::message::Message& msg, Key& key);
    metkit::mars::MarsRequest messageToRequest(const eckit::PathName& path, const char* verb = "retrieve");
    std::vector<metkit::mars::MarsRequest> messageToRequests(const eckit::PathName& path,
                                                             const char* verb = "retrieve");


private:

    virtual eckit::message::Message patch(const eckit::message::Message& msg);
    static void msgToKey(const eckit::message::Message& msg, Key& key);

    bool checkDuplicates_;
    std::set<Key> seen_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif

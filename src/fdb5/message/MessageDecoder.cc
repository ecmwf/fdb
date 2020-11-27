/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>
#include <cctype>
#include <sstream>
#include <iomanip>

#include "metkit/codes/OdbDecoder.h"
#include "metkit/odb/IdMapper.h"

#include "fdb5/message/MessageDecoder.h"

#include "eckit/message/Reader.h"
#include "eckit/message/Message.h"


namespace fdb5 {


namespace  {
class KeySetter : public eckit::message::MetadataGatherer {

    void setValue(const std::string& key, const std::string& value) override {
        key_.set(key, value);
    }

    void setValue(const std::string&, long) override {}

    void setValue(const std::string&, double) override {}

protected:
    Key& key_;

public:

    KeySetter(Key& key): key_(key) {
        ASSERT(key_.empty());
    }
};

class OdbKeySetter : public KeySetter {

    void setValue(const std::string& key, long value) override {
        std::string strValue;
        if (metkit::odb::IdMapper::instance().alphanumeric(key, value, strValue)) {
            key_.set(key, strValue);
        } else {
            if (key == "time") {
                std::stringstream ss;
                ss << std::setw(4) << std::setfill('0') << (value/100);
                key_.set(key, ss.str());
            } else {
                key_.set(key, std::to_string(value));
            }
        }
    }

public:

    OdbKeySetter(Key& key): KeySetter(key) {
    }
};
}  // namespace

//----------------------------------------------------------------------------------------------------------------------

MessageDecoder::MessageDecoder(bool checkDuplicates):
    checkDuplicates_(checkDuplicates) {
}

MessageDecoder::~MessageDecoder() {}

void msgToKey(const eckit::message::Message& msg, Key& key) {

    KeySetter* setter = metkit::codes::OdbDecoder::isOdb(msg) ? new OdbKeySetter(key) : new KeySetter(key);
    msg.getMetadata(*setter);
    delete setter;
}

void MessageDecoder::messageToKey(const eckit::message::Message& msg, Key& key) {

    eckit::message::Message patched = patch(msg);

    msgToKey(patched, key);

    if ( checkDuplicates_ ) {
        if ( seen_.find(key) != seen_.end() ) {
            std::ostringstream oss;
            oss << "Message has duplicate parameters in the same request: " << key;
            throw eckit::SeriousBug( oss.str() );
        }

        seen_.insert(key);
    }
}

metkit::mars::MarsRequest MessageDecoder::messageToRequest(const eckit::PathName &path, const char *verb) {
    metkit::mars::MarsRequest request(verb);

    for (auto& r: messageToRequests(path, verb))
        request.merge(r);

    return request;
}


std::vector<metkit::mars::MarsRequest> MessageDecoder::messageToRequests(const eckit::PathName &path, const char *verb) {

    std::vector<metkit::mars::MarsRequest> requests;
    eckit::message::Reader reader(path);
    eckit::message::Message msg;

    Key key;

    std::map<std::string, std::set<std::string> > s;

    while ( (msg = reader.next()) ) {

        key.clear();

        messageToKey(msg, key);

        requests.push_back(key.request(verb));
    }

    return requests;
}

eckit::message::Message MessageDecoder::patch(const eckit::message::Message& msg) {
    // Give a chance to subclasses to modify the message
    return msg;
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

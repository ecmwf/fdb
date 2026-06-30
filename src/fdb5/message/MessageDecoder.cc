/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <cctype>

#include "fdb5/message/MessageDecoder.h"

#include "eckit/message/Message.h"
#include "eckit/message/Reader.h"

#include "metkit/mars/Type.h"

namespace fdb5 {

namespace {
class KeySetter : public eckit::message::MetadataGatherer {

    void setValue(const std::string& key, const std::string& value) override { key_.set(key, value); }

    void setValue(const std::string& key, long value) override {
        if (const auto [iter, found] = key_.find(key); !found) {
            key_.set(key, std::to_string(value));
        }
    }

    void setValue(const std::string& key, double value) override {
        if (const auto [iter, found] = key_.find(key); !found) {
            key_.set(key, std::to_string(value));
        }
    }

protected:

    Key& key_;

public:

    KeySetter(Key& key) : key_(key) { ASSERT(key_.empty()); }
};

}  // namespace

//----------------------------------------------------------------------------------------------------------------------

MessageDecoder::MessageDecoder(bool checkDuplicates) : checkDuplicates_(checkDuplicates) {}

MessageDecoder::~MessageDecoder() {}

Key MessageDecoder::messageToKey(const eckit::message::Message& msg) {
    Key key;
    MessageDecoder::msgToKey(msg, key);
    return key;
}


void MessageDecoder::msgToKey(const eckit::message::Message& msg, Key& key) {

    KeySetter setter(key);
    msg.getMetadata(setter);

    key.unset("stepunits");
}

void MessageDecoder::messageToKey(const eckit::message::Message& msg, Key& key) {

    eckit::message::Message patched = patch(msg);

    msgToKey(patched, key);

    if (checkDuplicates_) {
        if (seen_.find(key) != seen_.end()) {
            std::ostringstream oss;
            oss << "Message has duplicate parameters in the same request: " << key;
            throw eckit::SeriousBug(oss.str());
        }

        seen_.insert(key);
    }
}

metkit::mars::MarsRequest MessageDecoder::messageToRequest(const eckit::PathName& path, const char* verb) {
    metkit::mars::MarsRequest request(verb);

    for (auto& r : messageToRequests(path, verb)) {
        request.merge(r);
    }

    return request;
}


std::vector<metkit::mars::MarsRequest> MessageDecoder::messageToRequests(const eckit::PathName& path,
                                                                         const char* verb) {

    std::vector<metkit::mars::MarsRequest> requests;
    eckit::message::Reader reader(path);
    eckit::message::Message msg;

    std::map<std::string, std::set<std::string>> s;

    while ((msg = reader.next())) {

        Key key;

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

}  // namespace fdb5

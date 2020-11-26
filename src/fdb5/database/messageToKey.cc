
#include "messageToKey.h"

#include <sstream>
#include <iomanip>

#include "eckit/message/Decoder.h"
#include "eckit/message/Message.h"
#include "eckit/exception/Exceptions.h"

#include "metkit/codes/OdbDecoder.h"
#include "metkit/odb/IdMapper.h"

#include "fdb5/database/Key.h"

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
        // The key must be clean at this point, as it is being returned (MARS-689)
        // TODO: asserting to ensure the the key is already cleared seems better
        key_.clear();
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
            }
        }
    }

public:

    OdbKeySetter(Key& key): KeySetter(key) {
    }
};
}  // namespace

Key messageToKey(const eckit::message::Message& msg) {
    Key key;

    KeySetter setter = metkit::codes::OdbDecoder::isOdb(msg) ? OdbKeySetter(key) : KeySetter(key);
    msg.getMetadata(setter);

    return key;
}

}  // namespace fdb5

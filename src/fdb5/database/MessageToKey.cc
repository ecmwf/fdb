
#include "MessageToKey.h"

#include <sstream>

#include "eckit/message/Message.h"
#include "eckit/exception/Exceptions.h"

#include "fdb5/database/Key.h"

namespace fdb5 {

namespace  {
class KeySetter : public eckit::message::MetadataGatherer {

    Key& key_;

    void setValue(const std::string& key, const std::string& value) override {
        key_.set(key, value);
    }

    void setValue(const std::string& /*key*/, long /*value*/) override {}

    void setValue(const std::string& /*key*/, double /*value*/) override {}

public:

    KeySetter(Key& key): key_(key) {
        // The key must be clean at this point, as it is being returned (MARS-689)
        // TODO: asserting to ensure the the key is already cleared seems better
        key_.clear();
    }
};
}  // namespace


MessageToKey::MessageToKey(bool checkDuplicates) : checkDuplicates_{checkDuplicates} {}

void MessageToKey::operator()(const eckit::message::Message& msg, Key& key) {
    KeySetter setter(key);
    msg.getMetadata(setter);

    // check for duplicated entries (within same request)

    if ( checkDuplicates_ ) {
        if ( seen_.find(key) != seen_.end() ) {
            std::ostringstream oss;
            oss << "Message has duplicate parameters in the same request: " << key;
            throw eckit::SeriousBug( oss.str() );
        }

        seen_.insert(key);
    }
}

}  // namespace fdb5

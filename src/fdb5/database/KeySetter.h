#ifndef fdb5_KeySetter_H
#define fdb5_KeySetter_H

#include "eckit/message/Message.h"
#include "fdb5/database/Key.h"

namespace fdb5 {

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

}  // namespace fdb5

#endif  // fdb5_KeySetter_H

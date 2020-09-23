#ifndef fdb5_MessageToKey_H
#define fdb5_MessageToKey_H

#include <set>

namespace eckit {
namespace message {
class Message;
}
}  // namespace eckit

namespace fdb5 {

class Key;

class MessageToKey {
    bool checkDuplicates_;
    std::set<Key> seen_;

public:
    MessageToKey(bool checkDuplicates = false);
    void operator()(const eckit::message::Message& msg, Key& key);
};

}  // namespace fdb5

#endif  // fdb5_MessageToKey_H

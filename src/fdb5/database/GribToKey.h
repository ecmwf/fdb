#ifndef fdb5_GribToKey_H
#define fdb5_GribToKey_H

#include <set>

namespace eckit {
namespace message {
class Message;
}
}  // namespace eckit

namespace fdb5 {

class Key;

class GribToKey {
    bool checkDuplicates_;
    std::set<Key> seen_;

public:
    GribToKey(bool checkDupes = false);
    void operator()(const eckit::message::Message& msg, Key& key);
};

}  // namespace fdb5

#endif  // fdb5_GribToKey_H

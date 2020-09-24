#ifndef fdb5_messageToKey_H
#define fdb5_messageToKey_H

namespace eckit {
namespace message {
class Message;
}
}  // namespace eckit

namespace fdb5 {

class Key;

Key messageToKey(const eckit::message::Message& msg);

}  // namespace fdb5

#endif  // fdb5_messageToKey_H

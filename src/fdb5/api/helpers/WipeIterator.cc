/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/helpers/WipeIterator.h"

#include "eckit/serialisation/Stream.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

WipeElement::WipeElement(WipeElementType type, const std::string& msg, eckit::URI uri) :
    type_(type), msg_(msg), uris_({uri}) {}

WipeElement::WipeElement(WipeElementType type, const std::string& msg) : type_(type), msg_(msg), uris_({}) {}

WipeElement::WipeElement(WipeElementType type, const std::string& msg, std::set<eckit::URI>&& uris) :
    type_(type), msg_(msg), uris_(std::move(uris)) {}

WipeElement::WipeElement(eckit::Stream& s) {
    unsigned char t;
    s >> t;
    type_ = static_cast<WipeElementType>(t);
    s >> msg_;
    size_t numURIs;
    s >> numURIs;
    for (size_t i = 0; i < numURIs; i++) {
        uris_.insert(eckit::URI{s});
    }
}

void WipeElement::print(std::ostream& out) const {
    out << msg_ << std::endl;
    if (type_ != CATALOGUE_INFO) {
        if (uris_.size() > 0) {
            for (const auto& uri : uris_) {
                out << "    " << uri.asString() << std::endl;
            }
            out << std::endl;
        }
        else {
            out << "    - NONE -" << std::endl;
        }
    }
}

size_t encodeSizeString(const std::string& s) {
    return (1 + 4 + s.size());
}

size_t WipeElement::encodeSize() const {
    size_t aux = 0;
    for (const auto& l : uris_) {
        aux += 256;
    }
    return 1 + 4 + encodeSizeString(msg_) + 1 + 4 + aux;
}

void WipeElement::encode(eckit::Stream& s) const {
    s << static_cast<unsigned char>(type_);
    s << msg_;
    s << uris_.size();
    for (const auto& uri : uris_) {
        s << uri;
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

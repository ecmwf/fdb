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

#include "eckit/log/Log.h"
#include "eckit/serialisation/Stream.h"

#include "fdb5/LibFdb5.h"

using namespace eckit;
namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

std::ostream& operator<<(std::ostream& s, const WipeElementType& t) {
    switch (t) {
        case WipeElementType::WIPE_CATALOGUE_INFO:
            s << "WIPE_CATALOGUE_INFO";
            break;
        case WipeElementType::WIPE_CATALOGUE:
            s << "WIPE_CATALOGUE";
            break;
        case WipeElementType::WIPE_CATALOGUE_SAFE:
            s << "WIPE_CATALOGUE_SAFE";
            break;
        case WipeElementType::WIPE_CATALOGUE_AUX:
            s << "WIPE_CATALOGUE_AUX";
            break;
        case WipeElementType::WIPE_STORE_INFO:
            s << "WIPE_STORE_INFO";
            break;
        case WipeElementType::WIPE_STORE:
            s << "WIPE_STORE";
            break;
        case WipeElementType::WIPE_STORE_URI:
            s << "WIPE_STORE_URI";
            break;
        case WipeElementType::WIPE_STORE_AUX:
            s << "WIPE_STORE_AUX";
            break;
    }
    s << "(" << ((int)t) << ")";
    return s;
}

WipeElement::WipeElement(WipeElementType type, const std::string& msg, eckit::URI uri) :
    type_(type), msg_(msg), uris_({uri}) {}

WipeElement::WipeElement(WipeElementType type, const std::string& msg, const std::vector<eckit::URI>& uris) :
    type_(type), msg_(msg), uris_(uris) {}

WipeElement::WipeElement(eckit::Stream& s) {
    unsigned char t;
    s >> t;
    type_ = static_cast<WipeElementType>(t);
    s >> msg_;
    size_t numURIs;
    s >> numURIs;
    for (size_t i = 0; i < numURIs; i++) {
        uris_.push_back(s);
    }
}

void WipeElement::print(std::ostream& out) const {

    LOG_DEBUG_LIB(LibFdb5) << "Wipe(type=" << type_ << ",msg=" << msg_ << ",uris=[";
    std::string sep = "";
    for (const auto& uri : uris_) {
        LOG_DEBUG_LIB(LibFdb5) << sep << std::endl << "        " << uri;
        sep = ",";
    }
    LOG_DEBUG_LIB(LibFdb5) << "])" << std::endl;


    out << msg_ << std::endl;
    if (type_ != WIPE_CATALOGUE_INFO && type_ != WIPE_STORE_INFO) {
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
    // size_t aux = 0;
    // for (const auto& l : uris_) {
    //     aux += 256;
    // }
    return 1 + 4 + encodeSizeString(msg_) + 1 + 4 + (256 * uris_.size());
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

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

#include "fdb5/api/helpers/ListIterator.h"

#include <utility>

#include "eckit/log/JSON.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

ListElement::ListElement(const std::vector<Key>& keyParts, std::shared_ptr<const FieldLocation> location, time_t timestamp) :
    keyParts_(keyParts), location_(std::move(location)), timestamp_(timestamp) {}

ListElement::ListElement(eckit::Stream &stream) {
    stream >> keyParts_;
    location_.reset(eckit::Reanimator<FieldLocation>::reanimate(stream));
    stream >> timestamp_;
}

Key ListElement::combinedKey() const {
    Key combined;

    for (const Key& partKey : keyParts_) {
        for (const auto& keyValue : partKey) {
            combined.set(keyValue.first, keyValue.second);
        }
    }

    return combined;
}

void ListElement::print(std::ostream &out, bool withLocation, bool withLength) const {
    if (!withLocation && location_ && !location_->host().empty()) {
        out << "host=" << location_->host() << ",";
    }
    for (const auto& bit : keyParts_) {
        out << bit;
    }
    if (location_) {
        if (withLocation) {
            out << " " << *location_;
        } else if (withLength) {
            out << ",length=" << location_->length();
        }
    }
}

void ListElement::json(eckit::JSON& json) const {
    json << combinedKey().keyDict();
    json << "length" << location_->length();
}

void ListElement::encode(eckit::Stream &stream) const {
    stream << keyParts_;
    stream << *location_;
    stream << timestamp_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

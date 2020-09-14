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

#include "eckit/log/JSON.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

ListElement::ListElement(const std::vector<Key>& keyParts, std::shared_ptr<Field> field, const Key& remapKey) :
    keyParts_(keyParts), field_(field), remapKey_(remapKey) {}


ListElement::ListElement(eckit::Stream &s) {
    s >> keyParts_;

    FieldLocation* location = eckit::Reanimator<FieldLocation>::reanimate(s);
    std::time_t tmp;
    s >> tmp;
    timestamp_t timestamp = std::chrono::system_clock::from_time_t(tmp);

    field_.reset(new Field(*location, timestamp));
    s >> remapKey_;
}

Key ListElement::combinedKey() const {
    Key combined;

    for (const Key& partKey : keyParts_) {
        for (const auto& kv : partKey) {
            combined.set(kv.first, kv.second);
        }
    }

    return combined;
}


void ListElement::print(std::ostream &out, bool withLocation) const {
    if (field_ && !withLocation) {
        out << "host=" << field_->location().host() << ",";
    }
    for (const auto& bit : keyParts_) {
        out << bit;
    }
    if (field_) {
        if (withLocation) {
            out << " " << field_->location();
        } else {
            out << ",length=" << field_->location().length();
        }
    }
}

void ListElement::json(eckit::JSON& json) const {
    json << combinedKey().keyDict();
    json << "length" << field_->location().length();
}

void ListElement::encode(eckit::Stream &s) const {
    s << keyParts_;
    s << field_->location();
    s << std::chrono::system_clock::to_time_t(field_->timestamp());
    s << remapKey_;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

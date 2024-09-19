/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/helpers/ListElement.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/log/JSON.h"
#include "eckit/serialisation/Reanimator.h"
#include "eckit/serialisation/Stream.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/KeyChain.h"

#include <cstddef>
#include <memory>
#include <ostream>
#include <utility>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

ListElement::ListElement(Key dbKey, const eckit::URI& uri, const TimeStamp& timestamp):
    keys_ {std::move(dbKey)}, uri_ {uri}, timestamp_ {timestamp} { }

ListElement::ListElement(Key dbKey, Key indexKey, const eckit::URI& uri, const TimeStamp& timestamp):
    keys_ {std::move(dbKey), std::move(indexKey)}, uri_ {uri}, timestamp_ {timestamp} { }

ListElement::ListElement(Key dbKey, Key indexKey, Key datumKey, std::shared_ptr<const FieldLocation> location,
                         const TimeStamp& timestamp):
    keys_ {std::move(dbKey), std::move(indexKey), std::move(datumKey)},
    loc_ {std::move(location)},
    timestamp_ {timestamp} { }

ListElement::ListElement(const KeyChain& keys, std::shared_ptr<const FieldLocation> location, const TimeStamp& timestamp):
    ListElement(keys[0], keys[1], keys[2], std::move(location), timestamp) { }

ListElement::ListElement(eckit::Stream& stream) {
    stream >> keys_;
    stream >> uri_;
    loc_.reset(eckit::Reanimator<FieldLocation>::reanimate(stream));
    stream >> timestamp_;
}

const FieldLocation& ListElement::location() const {
    if (!loc_) { throw eckit::SeriousBug("Only datum (3-level) elements have FieldLocation.", Here()); }
    return *loc_;
}

const eckit::URI& ListElement::uri() const {
    if (loc_) { return loc_->uri(); }
    return uri_;
}

eckit::Offset ListElement::offset() const {
    return loc_ ? loc_->offset() : eckit::Offset(0);
}

eckit::Length ListElement::length() const {
    return loc_ ? loc_->length() : eckit::Length(0);
}

void ListElement::print(std::ostream& out, const bool location, const bool length, const bool timestamp, const char* sep) const {
    out << keys_;
    if (location) {
        out << sep;
        if (loc_) {
            out << *loc_;
        } else {
            out << "host=" << (uri_.host().empty() ? "empty" : uri_.host());
        }
    }
    if (length) { out << sep << "length=" << this->length(); }
    if (timestamp) { out << sep << "timestamp=" << timestamp_; }
}

void ListElement::json(eckit::JSON& json) const {
    json << keys_.combine().keyDict();
    if (loc_) { json << "length" << loc_->length(); }
}

void ListElement::encode(eckit::Stream& stream) const {
    for (const auto& key : keys_) { stream << key; }
    stream << uri_;
    if (loc_) { stream << *loc_; }
    stream << timestamp_;
}

//----------------------------------------------------------------------------------------------------------------------
// friends

std::ostream& operator<<(std::ostream& out, const ListElement& elem) {
    elem.print(out, false, false, false, " ");
    return out;
}

eckit::Stream& operator<<(eckit::Stream& stream, const ListElement& elem) {
    elem.encode(stream);
    return stream;
}

eckit::JSON& operator<<(eckit::JSON& json, const ListElement& elem) {
    elem.json(json);
    return json;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

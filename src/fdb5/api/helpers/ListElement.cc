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

#include <cstddef>
#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------
// HELPERS

namespace {

template<std::size_t N, typename = std::enable_if_t<(N > 1 && N < 4)>>
auto combineKeys(const KeyChain<N>& keys) -> Key {
    /// @todo fixme: no API to get registry from Key; fix this copy hack
    auto combined = keys[0];
    for (const auto& key : keys) {
        for (auto&& [keyword, value] : key) { combined.set(keyword, value); }
    }
    return std::move(combined);
}

}  // namespace

//----------------------------------------------------------------------------------------------------------------------
// LIST ELEMENT

ListElement::ListElement(Key key, const eckit::URI& uri, const TimeStamp timestamp):
    key_ {std::move(key)}, keys_ {key_, Key {}, Key {}}, attributes_ {uri, timestamp} { }

ListElement::ListElement(IndexKeys&& keys, const eckit::URI& uri, const TimeStamp timestamp):
    key_ {combineKeys(keys)}, keys_ {std::move(keys[0]), std::move(keys[1]), Key {}}, attributes_ {uri, timestamp} { }

ListElement::ListElement(DatumKeys&& keys, const FieldLocation& location, const TimeStamp timestamp):
    key_ {combineKeys(keys)},
    keys_ {std::move(keys)},
    attributes_ {location.uri(), timestamp, location.offset(), location.length()},
    loc_ {location.make_shared()} { }

ListElement::ListElement(const DatumKeys& keys, const FieldLocation& location, const TimeStamp timestamp):
    key_ {combineKeys(keys)},
    keys_ {keys},
    attributes_ {location.uri(), timestamp, location.offset(), location.length()},
    loc_ {location.make_shared()} { }

ListElement::ListElement(eckit::Stream& stream) {
    for (auto& key : keys_) { stream >> key; }
    stream >> key_;
    stream >> attributes_;
    loc_.reset(eckit::Reanimator<FieldLocation>::reanimate(stream));
}

auto ListElement::location() const -> const FieldLocation& {
    if (!loc_) { throw eckit::SeriousBug("Only datum (3-level) elements have FieldLocation.", Here()); }
    return *loc_;
}

void ListElement::print(std::ostream& out, const bool location, const bool length, const bool timestamp, const char* sep) const {
    if (location) { out << "host=" << (attributes_.uri.host().empty() ? "empty" : attributes_.uri.host()) << sep; }
    for (const auto& key : keys_) {
        if (!key.empty()) { out << key; }
    }
    if (length) { out << sep << "length=" << attributes_.length; }
    if (timestamp) { out << sep << "timestamp=" << attributes_.timestamp; }
    if (location && loc_) { out << sep << *loc_; }
}

void ListElement::json(eckit::JSON& json) const {
    json << key_.keyDict();
    json << "length" << attributes_.length;
}

void ListElement::encode(eckit::Stream& stream) const {
    stream << key_;
    for (const auto& key : keys_) { stream << key; }
    stream << attributes_;
    if (loc_) { stream << *loc_; }
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

void operator>>(eckit::Stream& stream, ListElement::Attributes& attrs) {
    stream >> attrs.uri;
    stream >> attrs.timestamp;
    stream >> attrs.offset;
    stream >> attrs.length;
}

auto operator<<(eckit::Stream& stream, const ListElement::Attributes& attrs) -> eckit::Stream& {
    stream << attrs.uri;
    stream << attrs.timestamp;
    stream << attrs.offset;
    stream << attrs.length;
    return stream;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @author Emanuele Danovaro
/// @author Metin Cakircali
/// @date   October 2018

#pragma once

#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/KeyChain.h"

#include <ctime>
#include <iosfwd>
#include <memory>

namespace eckit {
class JSON;
class Stream;
}

namespace fdb5 {

class FieldLocation;

//----------------------------------------------------------------------------------------------------------------------

/// Define a standard object which can be used to iterate the results of a
/// list() call on an arbitrary FDB object

class ListElement {
public:  // types
    using TimeStamp = std::time_t;

public:  // methods
    ListElement(Key dbKey, const eckit::URI& uri, const TimeStamp& timestamp);

    ListElement(Key dbKey, Key indexKey, const eckit::URI& uri, const TimeStamp& timestamp);

    ListElement(Key dbKey, Key indexKey, Key datumKey, const FieldLocation& location, const TimeStamp& timestamp);

    ListElement(const KeyChain& keys, const FieldLocation& location, const TimeStamp& timestamp);

    explicit ListElement(eckit::Stream& stream);

    ListElement() = default;

    auto keys() const -> const KeyChain& { return keys_; }

    auto combinedKey() const -> const Key& { return keys_.combine(); }

    auto uri() const -> const eckit::URI& { return uri_; }

    auto timestamp() const -> const TimeStamp& { return timestamp_; }

    auto location() const -> const FieldLocation&;

    auto offset() const -> eckit::Offset;

    auto length() const -> eckit::Length;

    void print(std::ostream& out, bool location, bool length, bool timestamp, const char* sep) const;

private:  // methods
    void encode(eckit::Stream& stream) const;

    void json(eckit::JSON& json) const;

    friend std::ostream& operator<<(std::ostream& out, const ListElement& elem);

    friend eckit::Stream& operator<<(eckit::Stream& stream, const ListElement& elem);

    friend eckit::JSON& operator<<(eckit::JSON& json, const ListElement& elem);

private:  // members
    KeyChain keys_;

    eckit::URI uri_;

    std::shared_ptr<const FieldLocation> loc_;

    TimeStamp timestamp_ {0};
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

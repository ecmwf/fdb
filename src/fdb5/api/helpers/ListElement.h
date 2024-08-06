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

#include <array>
#include <cstddef>  // std::size_t
#include <ctime>    // std::time_t
#include <iosfwd>   // std::ostream
#include <memory>

namespace eckit {
class JSON;
class Stream;
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FieldLocation;

template<std::size_t N>
using KeyChain = std::array<Key, N>;

//----------------------------------------------------------------------------------------------------------------------

/// Define a standard object which can be used to iterate the results of a
/// list() call on an arbitrary FDB object

class ListElement {
public:  // types
    using IndexKeys = KeyChain<2>;
    using DatumKeys = KeyChain<3>;
    using TimeStamp = std::time_t;

    struct Attributes {
        eckit::URI    uri;
        TimeStamp     timestamp {0};
        eckit::Offset offset {0};
        eckit::Length length {0};

        friend void operator>>(eckit::Stream& stream, Attributes& attrs);
        friend auto operator<<(eckit::Stream& stream, const Attributes& attrs) -> eckit::Stream&;
    };

public:  // methods
    ListElement(Key key, const eckit::URI& uri, TimeStamp timestamp);

    ListElement(IndexKeys&& keys, const eckit::URI& uri, TimeStamp timestamp);

    ListElement(DatumKeys&& keys, const FieldLocation& location, TimeStamp timestamp);

    ListElement(const DatumKeys& keys, const FieldLocation& location, TimeStamp timestamp);

    explicit ListElement(eckit::Stream& stream);

    ListElement() = default;

    auto key() const -> const Key& { return key_; }

    auto keys() const -> const DatumKeys& { return keys_; }

    auto attributes() const -> const Attributes& { return attributes_; }

    auto location() const -> const FieldLocation&;

    void print(std::ostream& out, bool location, bool length, bool timestamp, const char* sep) const;

private:  // methods
    void encode(eckit::Stream& stream) const;

    void json(eckit::JSON& json) const;

    friend std::ostream& operator<<(std::ostream& out, const ListElement& elem);

    friend eckit::Stream& operator<<(eckit::Stream& stream, const ListElement& elem);

    friend eckit::JSON& operator<<(eckit::JSON& json, const ListElement& elem);

private:  // members
    Key key_;

    DatumKeys keys_;

    Attributes attributes_;

    std::shared_ptr<const FieldLocation> loc_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/helpers/ControlIterator.h"

#include "eckit/serialisation/Stream.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

eckit::Stream& operator<<(eckit::Stream& s, const ControlAction& a) {
    s << static_cast<typename std::underlying_type<ControlAction>::type>(a);
    return s;
}

eckit::Stream& operator>>(eckit::Stream& s, ControlAction& a) {
    typename std::underlying_type<ControlAction>::type tmp;
    s >> tmp;
    a = static_cast<ControlAction>(tmp);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

ControlIdentifierIterator::ControlIdentifierIterator(const ControlIdentifiers& identifiers) :
    value_(0),
    remaining_(identifiers.value_) {

    if (remaining_ != 0) {
        nextValue();
    }
}

ControlIdentifier ControlIdentifierIterator::operator*() const {
    return static_cast<ControlIdentifier>(value_);
}

bool ControlIdentifierIterator::operator==(const ControlIdentifierIterator& other) {
    return value_ == other.value_;
}

bool ControlIdentifierIterator::operator!=(const ControlIdentifierIterator& other) {
    return !(*this == other);
}

ControlIdentifierIterator& ControlIdentifierIterator::operator++() {
    nextValue();
    return *this;
}

void ControlIdentifierIterator::nextValue() {

    if (remaining_ == 0) {
        value_ = 0;
        return;
    }

    if (value_ == 0) value_ = 1;

    while ((remaining_ & value_) == 0) {
        value_ <<= 1;
        ASSERT(remaining_ >= value_);
    }

    remaining_ ^= value_;
}

//----------------------------------------------------------------------------------------------------------------------

ControlIdentifiers::ControlIdentifiers() :
    value_(0) {}

ControlIdentifiers::ControlIdentifiers(const ControlIdentifier& val) :
    value_(static_cast<value_type>(val)) {
}

ControlIdentifiers::ControlIdentifiers(eckit::Stream &s) {
    s >> value_;
}

ControlIdentifiers& ControlIdentifiers::operator|=(const ControlIdentifier& val) {
    value_ |= static_cast<value_type>(val);
    return *this;
}

ControlIdentifiers ControlIdentifiers::operator|(const ControlIdentifier& val) {
    return (ControlIdentifiers(*this) |= val);
}

void ControlIdentifiers::encode(eckit::Stream& s) const {
    s << value_;
}

bool ControlIdentifiers::has(const ControlIdentifier& val) const {
    return (value_ & static_cast<value_type>(val)) == static_cast<value_type>(val);
}

ControlIdentifierIterator ControlIdentifiers::begin() const {
    return ControlIdentifierIterator(*this);
}

ControlIdentifierIterator ControlIdentifiers::end() const {
    return ControlIdentifierIterator(ControlIdentifier::None);
}

ControlIdentifiers ControlIdentifiers::all() {
    ControlIdentifiers out(ControlIdentifier::List);
    out |= ControlIdentifier::Retrieve;
    out |= ControlIdentifier::Archive;
    out |= ControlIdentifier::Wipe;
    
    return out;
}

ControlIdentifiers operator|(const ControlIdentifier& lhs, const ControlIdentifier& rhs) {
    return (ControlIdentifiers(lhs) |= rhs);
}

void ControlIdentifiers::print( std::ostream &out ) const {
    std::string separator="";

    out << "ControlIdentifiers[";

    auto it = begin();
    while (it != end()) {
        out << separator << static_cast<value_type>(*it);
        separator = ",";
        ++it;
    }
    out << "]";
}

std::ostream &operator<<(std::ostream &s, const ControlIdentifiers &x) {
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

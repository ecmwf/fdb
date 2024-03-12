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

#include "fdb5/database/Catalogue.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

eckit::Stream& operator<<(eckit::Stream& stream, const ControlAction& controlAction) {
    stream << static_cast<typename std::underlying_type<ControlAction>::type>(controlAction);
    return stream;
}

eckit::Stream& operator>>(eckit::Stream& stream, ControlAction& controlAction) {
    typename std::underlying_type<ControlAction>::type tmp;
    stream >> tmp;
    controlAction = static_cast<ControlAction>(tmp);
    return stream;
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

bool ControlIdentifierIterator::operator==(const ControlIdentifierIterator& other) const {
    return value_ == other.value_;
}

bool ControlIdentifierIterator::operator!=(const ControlIdentifierIterator& other) const {
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

ControlIdentifiers::ControlIdentifiers(eckit::Stream &stream) {
    stream >> value_;
}

ControlIdentifiers& ControlIdentifiers::operator|=(const ControlIdentifier& val) {
    value_ |= static_cast<value_type>(val);
    return *this;
}

ControlIdentifiers ControlIdentifiers::operator|(const ControlIdentifier& val) {
    return (ControlIdentifiers(*this) |= val);
}

void ControlIdentifiers::encode(eckit::Stream& stream) const {
    stream << value_;
}

bool ControlIdentifiers::enabled(const ControlIdentifier& val) const {
    return (value_ & static_cast<value_type>(val)) == 0;
}

ControlIdentifierIterator ControlIdentifiers::begin() const {
    return ControlIdentifierIterator(*this);
}

ControlIdentifierIterator ControlIdentifiers::end() const {
    return ControlIdentifierIterator(ControlIdentifiers(ControlIdentifier::None));
}

ControlIdentifiers operator|(const ControlIdentifier& lhs, const ControlIdentifier& rhs) {
    return (ControlIdentifiers(lhs) |= rhs);
}

void ControlIdentifiers::print( std::ostream &out ) const {
    std::string separator;

    out << "ControlIdentifiers[";

    auto iterator = begin();
    while (iterator != end()) {
        out << separator << static_cast<value_type>(*iterator);
        separator = ",";
        ++iterator;
    }
    out << "]";
}

std::ostream &operator<<(std::ostream &stream, const ControlIdentifiers &controlIdentifiers) {
    controlIdentifiers.print(stream);
    return stream;
}

//----------------------------------------------------------------------------------------------------------------------

using value_type = typename std::underlying_type<ControlIdentifier>::type;

ControlElement::ControlElement(const Catalogue& catalogue) :
    key(catalogue.key()), location(catalogue.uri()) {
        
    controlIdentifiers = ControlIdentifiers(ControlIdentifier::None);

    for (auto controlIdentifier : ControlIdentifierList) {
        if (!catalogue.enabled(controlIdentifier)) controlIdentifiers |= controlIdentifier;
    }
}

ControlElement::ControlElement(eckit::Stream &stream) :
    key(stream),
    location(stream),
    controlIdentifiers(stream) {}

void ControlElement::encode(eckit::Stream& stream) const {
    stream << key;
    stream << location;
    stream << controlIdentifiers;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

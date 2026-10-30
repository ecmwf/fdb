// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

#include "fdb5/api/helpers/ControlIterator.h"

#include "eckit/serialisation/Stream.h"

#include "fdb5/database/Catalogue.h"

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
    value_(0), remaining_(identifiers.value_) {

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

    if (value_ == 0) {
        value_ = 1;
    }

    while ((remaining_ & value_) == 0) {
        value_ <<= 1;
        ASSERT(remaining_ >= value_);
    }

    remaining_ ^= value_;
}

//----------------------------------------------------------------------------------------------------------------------

ControlIdentifiers::ControlIdentifiers() : value_(0) {}

ControlIdentifiers::ControlIdentifiers(const ControlIdentifier& val) : value_(static_cast<value_type>(val)) {}

ControlIdentifiers::ControlIdentifiers(eckit::Stream& s) {
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

bool ControlIdentifiers::enabled(const ControlIdentifier& val) const {
    return !(value_ & static_cast<value_type>(val));
}

ControlIdentifierIterator ControlIdentifiers::begin() const {
    return ControlIdentifierIterator(*this);
}

ControlIdentifierIterator ControlIdentifiers::end() const {
    return ControlIdentifierIterator(ControlIdentifier::None);
}

ControlIdentifiers operator|(const ControlIdentifier& lhs, const ControlIdentifier& rhs) {
    return (ControlIdentifiers(lhs) |= rhs);
}

void ControlIdentifiers::print(std::ostream& out) const {
    std::string separator = "";

    out << "ControlIdentifiers[";

    auto it = begin();
    while (it != end()) {
        out << separator << static_cast<value_type>(*it);
        separator = ",";
        ++it;
    }
    out << "]";
}

std::ostream& operator<<(std::ostream& s, const ControlIdentifiers& x) {
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

using value_type = typename std::underlying_type<ControlIdentifier>::type;

ControlElement::ControlElement() : location() {}

ControlElement::ControlElement(const Catalogue& catalogue) :
    key(catalogue.key()),
    location(catalogue.uri()),
    lastRead(catalogue.lastRead()),
    lastWrite(catalogue.lastWrite()) {

    controlIdentifiers = ControlIdentifier::None;
    for (auto id : ControlIdentifierList) {
        if (!catalogue.enabled(id)) {
            controlIdentifiers |= id;
        }
    }
}

ControlElement::ControlElement(eckit::Stream& s) : key(s), location(s), controlIdentifiers(s) {
    int64_t read;
    int64_t write;
    s >> read;
    s >> write;
    lastRead = static_cast<time_t>(read);
    lastWrite = static_cast<time_t>(write);
}

void ControlElement::encode(eckit::Stream& s) const {
    s << key;
    s << location;
    s << controlIdentifiers;
    s << static_cast<int64_t>(lastRead);
    s << static_cast<int64_t>(lastWrite);
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/helpers/AccessIterator.h"

#include <ostream>

#include "eckit/serialisation/Stream.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

AccessIdentifiers::AccessIdentifiers() : value_(0) {}

AccessIdentifiers::AccessIdentifiers(const AccessIdentifier& val) : value_(static_cast<value_type>(val)) {}

AccessIdentifiers::AccessIdentifiers(eckit::Stream& s) {
    s >> value_;
}

AccessIdentifiers& AccessIdentifiers::operator|=(const AccessIdentifier& val) {
    value_ |= static_cast<value_type>(val);
    return *this;
}

AccessIdentifiers AccessIdentifiers::operator|(const AccessIdentifier& val) {
    return (AccessIdentifiers(*this) |= val);
}

void AccessIdentifiers::encode(eckit::Stream& s) const {
    s << value_;
}

bool AccessIdentifiers::has(const AccessIdentifier& val) const {
    return (value_ & static_cast<value_type>(val)) != 0;
}

bool AccessIdentifiers::empty() const {
    return value_ == 0;
}

AccessIdentifiers operator|(const AccessIdentifier& lhs, const AccessIdentifier& rhs) {
    return (AccessIdentifiers(lhs) |= rhs);
}

void AccessIdentifiers::print(std::ostream& out) const {

    const char* separator = "";

    out << "AccessIdentifiers[";

    if (has(AccessIdentifier::Read)) {
        out << separator << "read";
        separator = ",";
    }

    if (has(AccessIdentifier::Write)) {
        out << separator << "write";
    }

    out << "]";
}

std::ostream& operator<<(std::ostream& s, const AccessIdentifiers& x) {
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

AccessTime::AccessTime() : source_(Source::Now), when_(0) {}

AccessTime AccessTime::catalogueTime() {
    AccessTime t;
    t.source_ = Source::Catalogue;
    return t;
}

AccessTime AccessTime::at(time_t when) {
    AccessTime t;
    t.source_ = Source::Explicit;
    t.when_ = when;
    return t;
}

AccessTime::AccessTime(eckit::Stream& s) {
    value_type source;
    int64_t when;
    s >> source;
    s >> when;
    source_ = static_cast<Source>(source);
    when_ = static_cast<time_t>(when);
}

void AccessTime::encode(eckit::Stream& s) const {
    s << static_cast<value_type>(source_);
    s << static_cast<int64_t>(when_);
}

time_t AccessTime::resolve(time_t catalogueTime) const {
    switch (source_) {
        case Source::Catalogue:
            return catalogueTime;
        case Source::Explicit:
            return when_;
        case Source::Now:
        default:
            // 0 is used to mean 'now' by the relevant system calls
            return 0;
    }
}

void AccessTime::print(std::ostream& out) const {
    switch (source_) {
        case Source::Catalogue:
            out << "AccessTime[catalogue]";
            break;
        case Source::Explicit:
            out << "AccessTime[" << when_ << "]";
            break;
        default:
            out << "AccessTime[now]";
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

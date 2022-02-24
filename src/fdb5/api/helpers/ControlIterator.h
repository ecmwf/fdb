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
/// @date   June 2019

#pragma once

#include <cstdint>

#include "fdb5/api/helpers/APIIterator.h"

#include "fdb5/api/helpers/StatusIterator.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

enum class ControlAction : uint16_t {
    None = 0,

    Lock,
    Unlock
};

eckit::Stream& operator<<(eckit::Stream& s, const ControlAction& a);
eckit::Stream& operator>>(eckit::Stream& s, ControlAction& a);

//----------------------------------------------------------------------------------------------------------------------

enum class ControlIdentifier : uint16_t {
    None = 0,

    List      = 1 << 0,
    Retrieve  = 1 << 1,
    Archive   = 1 << 2,
    Wipe      = 1 << 3
};

//----------------------------------------------------------------------------------------------------------------------

// An iterator to facilitate working with the ControlIdentifiers structure

class ControlIdentifiers;

class ControlIdentifierIterator {
    using value_type = typename std::underlying_type<ControlIdentifier>::type;
    value_type value_;
    value_type remaining_;

public: // methods

    ControlIdentifierIterator(const ControlIdentifiers& identifiers);

    ControlIdentifier operator*() const;

    bool operator==(const ControlIdentifierIterator& other);
    bool operator!=(const ControlIdentifierIterator& other);

    ControlIdentifierIterator& operator++();

private: // methods

    void nextValue();
};


//----------------------------------------------------------------------------------------------------------------------

// A collection of the identified ControlIdentifier objects

class ControlIdentifiers {
    using value_type = typename std::underlying_type<ControlIdentifier>::type;
    value_type value_;

public:
    ControlIdentifiers();
    ControlIdentifiers(const ControlIdentifiers& val) = default;
    ControlIdentifiers(const ControlIdentifier& val);
    ControlIdentifiers(eckit::Stream& s);

    ControlIdentifiers& operator|=(const ControlIdentifier& val);
    ControlIdentifiers operator|(const ControlIdentifier& val);

    ControlIdentifierIterator begin() const;
    ControlIdentifierIterator end() const;

private:
    void encode(eckit::Stream& s) const;

    friend eckit::Stream& operator<<(eckit::Stream& s, const ControlIdentifiers& i) {
        i.encode(s);
        return s;
    }

    friend class ControlIdentifierIterator;
};

ControlIdentifiers operator|(const ControlIdentifier& lhs, const ControlIdentifier& rhs);

//----------------------------------------------------------------------------------------------------------------------

using ControlElement = StatusElement;

using ControlIterator = APIIterator<ControlElement>;

using ControlAggregateIterator = APIAggregateIterator<ControlElement>;

using ControlAsyncIterator = APIAsyncIterator<ControlElement>;

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

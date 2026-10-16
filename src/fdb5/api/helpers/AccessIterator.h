/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include <cstdint>
#include <ctime>
#include <iosfwd>
#include <type_traits>

namespace eckit {
class Stream;
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// Identify which types of meaningful access are being considered (read/write)

enum class AccessIdentifier : uint16_t {
    None = 0,
    Read = 1 << 0,
    Write = 1 << 1
};

//----------------------------------------------------------------------------------------------------------------------

class AccessIdentifiers {
    using value_type = typename std::underlying_type<AccessIdentifier>::type;
    value_type value_;

public:

    AccessIdentifiers();
    AccessIdentifiers(const AccessIdentifier& val);
    AccessIdentifiers(eckit::Stream& s);

    AccessIdentifiers& operator|=(const AccessIdentifier& val);
    AccessIdentifiers operator|(const AccessIdentifier& val);

    /// Whether @p val is among those selected.
    bool has(const AccessIdentifier& val) const;

    /// Whether nothing at all is selected.
    bool empty() const;

protected:  // methods

    friend std::ostream& operator<<(std::ostream& s, const AccessIdentifiers& x);
    void print(std::ostream& out) const;

private:

    void encode(eckit::Stream& s) const;

    friend eckit::Stream& operator<<(eckit::Stream& s, const AccessIdentifiers& i) {
        i.encode(s);
        return s;
    }
};

AccessIdentifiers operator|(const AccessIdentifier& lhs, const AccessIdentifier& rhs);

//----------------------------------------------------------------------------------------------------------------------

/// Specify the timestamp to be used in touch operations (intent rather than resolved time)

class AccessTime {
    using value_type = uint16_t;

public:

    enum class Source : value_type {
        Now = 0,   ///< the current time
        Catalogue, ///< best approximation from the catalogue (normally the mtime of the toc)
        Explicit   ///< the instant given to at()
    };

    /// Defaults to Now, so that an unqualified touch behaves as it always has.
    AccessTime();

    static AccessTime catalogueTime();
    static AccessTime at(time_t when);

    explicit AccessTime(eckit::Stream& s);

    Source source() const { return source_; }
    time_t explicitTime() const { return when_; }
    time_t resolve(time_t catalogueTime) const;

private:

    Source source_{Source::Now};
    time_t when_{0};

    void print(std::ostream& out) const;

    friend std::ostream& operator<<(std::ostream& s, const AccessTime& t) {
        t.print(s);
        return s;
    }

    void encode(eckit::Stream& s) const;

    friend eckit::Stream& operator<<(eckit::Stream& s, const AccessTime& t) {
        t.encode(s);
        return s;
    }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

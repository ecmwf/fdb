// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   DbStats.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#pragma once

#include <iosfwd>

#include "eckit/log/Statistics.h"
#include "eckit/memory/Counted.h"
#include "eckit/serialisation/Streamable.h"

namespace eckit {
class JSON;
}  // namespace eckit

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DbStatsContent : public eckit::Counted, public eckit::Statistics, public eckit::Streamable {
public:

    ~DbStatsContent() override;

    virtual void add(const DbStatsContent&) = 0;

    virtual void report(std::ostream& out, const char* indent) const = 0;
    virtual void json(eckit::JSON& json) const = 0;

public:  // For Streamable

    void encode(eckit::Stream& s) const override = 0;

protected:  // For Streamable

    static eckit::ClassSpec classSpec_;
};

//----------------------------------------------------------------------------------------------------------------------

/// @invariant content is always valid by contruction

class DbStats {

public:  // methods

    DbStats();
    DbStats(DbStatsContent*);

    ~DbStats();

    DbStats(const DbStats&);

    DbStats& operator=(const DbStats&);

    DbStats& operator+=(const DbStats& rhs);

    void add(const DbStats&);

    void report(std::ostream& out, const char* indent = "") const;
    void json(eckit::JSON& json) const;

    //    template <class T>
    //    T& as() {
    //        return dynamic_cast<T&>(*content_);
    //    }

private:  // methods

    void print(std::ostream&) const;
    void encode(eckit::Stream& s) const;

    friend std::ostream& operator<<(std::ostream& s, const DbStats& o) {
        o.print(s);
        return s;
    }

    friend eckit::Stream& operator<<(eckit::Stream& s, const DbStats& r) {
        r.encode(s);
        return s;
    }

private:  // members

    DbStatsContent* content_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5


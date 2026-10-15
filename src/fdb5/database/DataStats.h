// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @file   DataStats.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#pragma once

#include <iosfwd>

#include "eckit/log/Statistics.h"
#include "eckit/memory/Counted.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DataStatsContent : public eckit::Counted, public eckit::Statistics {
public:

    ~DataStatsContent() override;

    virtual void add(const DataStatsContent&) = 0;

    virtual void report(std::ostream& out, const char* indent) const = 0;
};

//----------------------------------------------------------------------------------------------------------------------

/// @invariant content is always valid by contruction

class DataStats {

public:  // methods

    DataStats();
    DataStats(DataStatsContent*);

    ~DataStats();

    DataStats(const DataStats&);

    DataStats& operator=(const DataStats&);

    void add(const DataStats&);

    void report(std::ostream& out, const char* indent = "") const;

    //    template <class T>
    //    T& as() {
    //        return dynamic_cast<T&>(*content_);
    //    }

private:  // methods

    void print(std::ostream&) const;

    friend std::ostream& operator<<(std::ostream& s, const DataStats& o) {
        o.print(s);
        return s;
    }

private:  // members

    DataStatsContent* content_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5


/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   DataStats.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   April 2016

#ifndef fdb5_DataStats_H
#define fdb5_DataStats_H

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

#endif

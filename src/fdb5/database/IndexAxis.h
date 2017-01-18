/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   IndexAxis.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#ifndef fdb5_IndexAxis_H
#define fdb5_IndexAxis_H

#include <iosfwd>
#include <set>
#include <map>

#include "eckit/memory/NonCopyable.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/types/Types.h"

namespace eckit {
class Stream;
}

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class IndexAxis : private eckit::NonCopyable {

public: // methods

    IndexAxis();
    IndexAxis(eckit::Stream &);

    ~IndexAxis();

    void insert(const Key &key);
    void encode(eckit::Stream &s) const;

    const eckit::StringSet &values(const std::string &keyword) const;

    void dump(std::ostream &out, const char* indent) const;

    friend std::ostream &operator<<(std::ostream &s, const IndexAxis &x) {
        x.print(s);
        return s;
    }

private: // methods

    void print(std::ostream &out) const;


private: // members

    typedef std::map<std::string, eckit::StringSet> AxisMap;
    AxisMap axis_;

    bool readOnly_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

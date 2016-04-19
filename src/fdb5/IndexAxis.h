/*
 * (C) Copyright 1996-2016 ECMWF.
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

namespace eckit { class JSON; }

namespace fdb5 {

class Key;

//----------------------------------------------------------------------------------------------------------------------

class IndexAxis : private eckit::NonCopyable {

public: // methods

    IndexAxis( const eckit::PathName& path );

    ~IndexAxis();

    void insert(const Key& key);

    const eckit::StringSet& values(const std::string& keyword) const;

    friend std::ostream& operator<<(std::ostream& s,const IndexAxis& x) { x.print(s); return s; }

private: // methods

    void print(std::ostream& out) const;

    void json(eckit::JSON&) const;

private: // members

    typedef std::map<std::string, eckit::StringSet> AxisMap;

    AxisMap axis_;

    eckit::PathName path_;

    bool readOnly_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

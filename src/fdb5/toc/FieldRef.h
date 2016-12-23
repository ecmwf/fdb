/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date Sep 2012

#ifndef fdb5_FieldRef_H
#define fdb5_FieldRef_H

#include "eckit/eckit.h"

#include "eckit/io/DataHandle.h"
#include "eckit/io/Length.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/io/Offset.h"
#include "eckit/filesystem/PathName.h"

#include "fdb5/database/FieldDetails.h"

namespace fdb5 {

class Field;
class FileStore;


//----------------------------------------------------------------------------------------------------------------------


class FieldRefLocation {

public:
    typedef size_t PathID;

    FieldRefLocation();
    FieldRefLocation(FileStore &, const Field &);


    PathID pathId() const { return pathId_; }
    const eckit::Offset &offset() const { return offset_; }
    const eckit::Length &length() const { return length_; }

protected:
    PathID          pathId_;
    eckit::Offset   offset_;
    eckit::Length   length_;

    void print(std::ostream &s) const;

    friend std::ostream &operator<<(std::ostream &s, const FieldRefLocation &x) {
        x.print(s);
        return s;
    }
};


//----------------------------------------------------------------------------------------------------------------------


class FieldRef;


class FieldRefReduced {
    FieldRefLocation location_;
public:
    FieldRefReduced();
    FieldRefReduced(const FieldRef&);
    const FieldRefLocation& location() const { return location_; }
};

class FieldRef  {
    FieldRefLocation location_;
    FieldDetails details_;
public:

    FieldRef();
    FieldRef(FileStore &, const Field &);

    FieldRef(const FieldRefReduced&);

    FieldRefLocation::PathID pathId() const { return location_.pathId(); }
    const eckit::Offset &offset() const { return location_.offset(); }
    const eckit::Length &length() const { return location_.length(); }

    const FieldRefLocation& location() const { return location_; }
    const FieldDetails& details() const { return details_; }

};

typedef FieldRef FieldRefFull;

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

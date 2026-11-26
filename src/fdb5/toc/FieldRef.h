// SPDX-FileCopyrightText: 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)
// SPDX-License-Identifier: Apache-2.0

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date Sep 2012

#pragma once

#include "eckit/eckit.h"

#include "eckit/filesystem/PathName.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"

#include "fdb5/database/FieldDetails.h"

namespace fdb5 {

class Field;
class UriStore;


//----------------------------------------------------------------------------------------------------------------------


class FieldRefLocation {

public:

    typedef size_t UriID;

    FieldRefLocation();
    FieldRefLocation(UriStore&, const Field&);


    UriID uriId() const { return uriId_; }
    const eckit::Offset& offset() const { return offset_; }
    const eckit::Length& length() const { return length_; }

protected:

    UriID uriId_;
    eckit::Offset offset_;
    eckit::Length length_;

    void print(std::ostream& s) const;

    friend std::ostream& operator<<(std::ostream& s, const FieldRefLocation& x) {
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

private:  // methods

    void print(std::ostream& s) const;

    friend std::ostream& operator<<(std::ostream& s, const FieldRefReduced& x) {
        x.print(s);
        return s;
    }
};

class FieldRef {
    FieldRefLocation location_;
    FieldDetails details_;

public:

    FieldRef();
    FieldRef(UriStore&, const Field&);

    FieldRef(const FieldRefReduced&);

    FieldRefLocation::UriID uriId() const { return location_.uriId(); }
    const eckit::Offset& offset() const { return location_.offset(); }
    const eckit::Length& length() const { return location_.length(); }

    const FieldRefLocation& location() const { return location_; }
    const FieldDetails& details() const { return details_; }

private:  // methods

    void print(std::ostream& s) const;

    friend std::ostream& operator<<(std::ostream& s, const FieldRef& x) {
        x.print(s);
        return s;
    }
};

typedef FieldRef FieldRefFull;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

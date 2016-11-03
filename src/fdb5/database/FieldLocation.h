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
/// @author Simon Smart
/// @date Nov 2016

#ifndef fdb5_FieldLocation_H
#define fdb5_FieldLocation_H

#include "eckit/eckit.h"

#include "eckit/io/Length.h"
#include "eckit/io/DataHandle.h"
#include "eckit/memory/SharedPtr.h"
#include "eckit/memory/Owned.h"

namespace fdb5 {


//----------------------------------------------------------------------------------------------------------------------

class FieldLocationVisitor;


class FieldLocation : public eckit::OwnedLock {

public: // methods

    FieldLocation();
    FieldLocation(eckit::Length length );
    FieldLocation(const FieldLocation& rhs);

    const eckit::Length &length() const { return length_; }

    virtual eckit::DataHandle *dataHandle() const = 0;

    /// Create a (shared) copy of the current object, for storage in a general container.
    virtual eckit::SharedPtr<FieldLocation> make_shared() const = 0;

    virtual void visit(FieldLocationVisitor& visitor) const = 0;

private: // methods

    void print( std::ostream &out ) const;

protected: // members

    eckit::Length length_;

private: // friends

    friend std::ostream &operator<<(std::ostream &s, const FieldLocation &x) {
        x.print(s);
        return s;
    }
};


class TocFieldLocation;

class FieldLocationVisitor {

public: // methods

    virtual void operator() (const FieldLocation& location);
    virtual void operator() (const TocFieldLocation& location);

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

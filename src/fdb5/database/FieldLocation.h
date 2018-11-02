/*
 * (C) Copyright 1996- ECMWF.
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
#include "eckit/filesystem/PathName.h"
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

    virtual eckit::PathName url() const = 0;

    const eckit::Length &length() const { return length_; }

    virtual eckit::DataHandle *dataHandle() const = 0;

    /// Create a (shared) copy of the current object, for storage in a general container.
    virtual std::shared_ptr<FieldLocation> make_shared() const = 0;

    virtual void visit(FieldLocationVisitor& visitor) const = 0;

    virtual void dump(std::ostream &out) const = 0;

private: // methods

    virtual void print( std::ostream &out ) const = 0;

protected: // members

    eckit::Length length_;

private: // friends

    friend std::ostream &operator<<(std::ostream &s, const FieldLocation &x) {
        x.print(s);
        return s;
    }
};


//----------------------------------------------------------------------------------------------------------------------


class FieldLocationVisitor : private eckit::NonCopyable {

public: // methods

    virtual ~FieldLocationVisitor();

    virtual void operator() (const FieldLocation& location) = 0;

};

class FieldLocationPrinter : public FieldLocationVisitor {
public:
    FieldLocationPrinter(std::ostream& out) : out_(out) {}
    virtual void operator() (const FieldLocation& location);
private:
    std::ostream& out_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif

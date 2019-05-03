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

#include <memory>

#include "eckit/filesystem/PathName.h"
#include "eckit/io/Length.h"
#include "eckit/memory/Owned.h"
#include "eckit/serialisation/Streamable.h"

namespace eckit {
    class DataHandle;
}

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FieldLocationVisitor;
class Key;


class FieldLocation : public eckit::OwnedLock, public eckit::Streamable {

public: // methods

    FieldLocation();
    FieldLocation(eckit::Length length );
    FieldLocation(eckit::Stream&);

    FieldLocation(const FieldLocation&) = delete;
    FieldLocation& operator=(const FieldLocation&) = delete;

    virtual eckit::PathName url() const = 0;

    const eckit::Length &length() const { return length_; }

    virtual eckit::DataHandle *dataHandle() const = 0;
    virtual eckit::DataHandle *dataHandle(const Key& remapKey) const = 0;

    /// Create a (shared) copy of the current object, for storage in a general container.
    virtual std::shared_ptr<FieldLocation> make_shared() const = 0;

    virtual std::shared_ptr<FieldLocation> stableLocation() const { return make_shared(); }

    virtual void visit(FieldLocationVisitor& visitor) const = 0;

    virtual void dump(std::ostream &out) const = 0;

private: // methods

    virtual void print( std::ostream &out ) const = 0;

protected: // For Streamable

    virtual void encode(eckit::Stream&) const;

    static eckit::ClassSpec                  classSpec_;

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

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/URI.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/serialisation/Streamable.h"

#ifndef fdb5_IndexLocation_H
#define fdb5_IndexLocation_H

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class IndexLocation : public eckit::Streamable {

public:  // methods

    IndexLocation();
    ~IndexLocation() override;

    //    virtual eckit::PathName path() const = 0;
    virtual eckit::URI uri() const = 0;

    virtual IndexLocation* clone() const = 0;

private:  // methods

    virtual void print(std::ostream& out) const = 0;


protected:  // For streamable

    void encode(eckit::Stream&) const override = 0;

    static eckit::ClassSpec classSpec_;

private:  // friends

    friend std::ostream& operator<<(std::ostream& s, const IndexLocation& x) {
        x.print(s);
        return s;
    }
};

//----------------------------------------------------------------------------------------------------------------------

class IndexLocationVisitor {

public:  // methods

    virtual ~IndexLocationVisitor();

    virtual void operator()(const IndexLocation&) = 0;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif  // fdb5_IndexLocation_H

/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "eckit/memory/NonCopyable.h"
#include "eckit/filesystem/PathName.h"

#ifndef fdb5_IndexLocation_H
#define fdb5_IndexLocation_H

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class IndexLocation : private eckit::NonCopyable {

public: // methods

    virtual ~IndexLocation();

    virtual eckit::PathName location() const = 0;

};

//----------------------------------------------------------------------------------------------------------------------

class IndexLocationVisitor {

public: // methods

    virtual ~IndexLocationVisitor();

    virtual void operator() (const IndexLocation&) = 0;
};

//----------------------------------------------------------------------------------------------------------------------

}

#endif // fdb5_IndexLocation_H

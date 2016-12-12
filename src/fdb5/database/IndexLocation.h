/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#ifndef fdb5_IndexLocation_H
#define fdb5_IndexLocation_H

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// A base class to support the various visitors
/// TODO: Do we (strictly) even need to have a base class?

class IndexLocation {

};

//----------------------------------------------------------------------------------------------------------------------

class TocIndexLocation;
namespace pmem {
    class PMemIndexLocation;
}

class IndexLocationVisitor {

public: // methods

    /// These are base methods (with default behaviour) for a visitor-pattern dispatch
    virtual void operator() (const IndexLocation& location);
    virtual void operator() (const TocIndexLocation& location);
    virtual void operator() (const pmem::PMemIndexLocation& location);

};

//----------------------------------------------------------------------------------------------------------------------

}

#endif // fdb5_IndexLocation_H

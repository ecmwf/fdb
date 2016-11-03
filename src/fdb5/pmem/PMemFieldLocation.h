/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date Nov 2016

#ifndef fdb5_pmem_PMemFieldLocation_H
#define fdb5_pmem_PMemFieldLocation_H

#include "eckit/memory/SharedPtr.h"

#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/FileStore.h"
#include "fdb5/database/FieldRef.h"

namespace fdb5 {
namespace pmem {


//----------------------------------------------------------------------------------------------------------------------

class PMemFieldLocation : public FieldLocation {
public:

    PMemFieldLocation();
    PMemFieldLocation(const TocFieldLocation& rhs);

    virtual eckit::DataHandle *dataHandle() const;

    virtual eckit::SharedPtr<FieldLocation> make_shared() const;

    virtual void visit(FieldLocationVisitor& visitor) const;

private: // methods

    void print(std::ostream &out) const;

private: // members

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif // fdb5_pmem_PMemFieldLocation_H

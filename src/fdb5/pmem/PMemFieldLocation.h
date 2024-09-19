/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/*
 * This software was developed as part of the EC H2020 funded project NextGenIO
 * (Project ID: 671951) www.nextgenio.eu
 */

/// @author Tiago Quintino
/// @author Simon Smart
/// @date Nov 2016

#ifndef fdb5_pmem_PMemFieldLocation_H
#define fdb5_pmem_PMemFieldLocation_H

#include "pmem/PersistentPtr.h"

#include "fdb5/database/FieldLocation.h"

#include <memory>

namespace fdb5 {
namespace pmem {

class PDataNode;
class DataPool;

//----------------------------------------------------------------------------------------------------------------------

class PMemFieldLocation : public FieldLocation {
public:

    PMemFieldLocation(const PMemFieldLocation& rhs);
    PMemFieldLocation(const ::pmem::PersistentPtr<PDataNode>& dataNode, DataPool& pool);

    ::pmem::PersistentPtr<PDataNode> node() const;

    virtual eckit::PathName url() const;

    virtual eckit::DataHandle *dataHandle() const;
    virtual eckit::DataHandle *dataHandle(const Key& remapKey) const;

    virtual std::shared_ptr<const FieldLocation> make_shared() const;

    // The PMemFieldLocation only has validity equal to that of the PMemDB.
    // This is a problem with any async API functions.
    // --> For visibility purposes return something more stable (currently a
    //     TocFieldLocation....)
    virtual std::shared_ptr<const FieldLocation> stableLocation() const;

    virtual void visit(FieldLocationVisitor& visitor) const;

    DataPool& pool() const;

protected: // For Streamable (see comments. This is a bit odd).

    virtual void encode(eckit::Stream&) const override;

private: // methods

    virtual void dump(std::ostream &out) const;

    virtual void print(std::ostream &out) const override;

private: // members

    ::pmem::PersistentPtr<PDataNode> dataNode_;

    DataPool& dataPool_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif // fdb5_pmem_PMemFieldLocation_H

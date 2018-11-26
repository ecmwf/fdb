/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
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

    PMemFieldLocation();
    PMemFieldLocation(const PMemFieldLocation& rhs);
    PMemFieldLocation(const ::pmem::PersistentPtr<PDataNode>& dataNode, DataPool& pool);
    PMemFieldLocation(eckit::Stream&);

    ::pmem::PersistentPtr<PDataNode> node() const;

    virtual eckit::PathName url() const;

    virtual eckit::DataHandle *dataHandle() const;

    virtual std::shared_ptr<FieldLocation> make_shared() const;

    virtual void visit(FieldLocationVisitor& visitor) const;

    DataPool& pool() const;

public: // For Streamable

    static const eckit::ClassSpec&  classSpec() { return classSpec_;}

protected: // For Streamable

    virtual void encode(eckit::Stream&) const;
    virtual const eckit::ReanimatorBase& reanimator() const { return reanimator_; }

    static eckit::ClassSpec                    classSpec_;
    static eckit::Reanimator<PMemFieldLocation> reanimator_;

private: // methods

    virtual void dump(std::ostream &out) const;

    virtual void print(std::ostream &out) const;

private: // members

    ::pmem::PersistentPtr<PDataNode> dataNode_;

    // This is a non-owning pointer. Acts as a nullable reference.
    DataPool* dataPool_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5

#endif // fdb5_pmem_PMemFieldLocation_H

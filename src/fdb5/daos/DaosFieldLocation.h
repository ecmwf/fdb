/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Nicolau Manubens
/// @date Nov 2022

#pragma once

// #include "eckit/filesystem/PathName.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"

#include "fdb5/database/FieldLocation.h"
// #include "fdb5/database/FileStore.h"
// #include "fdb5/toc/FieldRef.h"

#include "fdb5/daos/DaosName.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class DaosFieldLocation : public FieldLocation {
public:

// //     //RadosFieldLocation();
    DaosFieldLocation(const DaosFieldLocation& rhs);
//     DaosFieldLocation(const eckit::PathName path, eckit::Offset offset, eckit::Length length);
    // DaosFieldLocation(const eckit::URI &uri);
    DaosFieldLocation(const eckit::URI& uri, eckit::Offset offset, eckit::Length length, const Key& remapKey);
    // DaosFieldLocation(const FileStore& store, const FieldRef& ref);
    // DaosFieldLocation(eckit::Stream&);

// //    const eckit::PathName path() const { return uri_.name(); }
// //    const eckit::Offset&   offset() const { return offset_; }

    eckit::DataHandle* dataHandle() const override;
    // eckit::DataHandle* dataHandle(const Key& remapKey) const override;

    // // eckit::URI uri() const override;

    virtual std::shared_ptr<FieldLocation> make_shared() const override;

    virtual void visit(FieldLocationVisitor& visitor) const override;

public: // For Streamable

    // static const eckit::ClassSpec&  classSpec() { return classSpec_;}

protected: // For Streamable

    // virtual const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }
    // //virtual void encode(eckit::Stream&) const override;

    // static eckit::ClassSpec                    classSpec_;
    // static eckit::Reanimator<RadosFieldLocation> reanimator_;

private: // methods

// //    void dump(std::ostream &out) const override;

    void print(std::ostream &out) const override;

    // eckit::URI uri(const eckit::PathName &path);

private: // members

//    eckit::PathName path_;
//    eckit::Offset offset_;

//     For streamability
};


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Simon Smart
/// @date Nov 2016

#ifndef fdb5_TocFieldLocation_H
#define fdb5_TocFieldLocation_H

#include "eckit/filesystem/PathName.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"

#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/UriStore.h"
#include "fdb5/toc/FieldRef.h"

namespace fdb5 {


//----------------------------------------------------------------------------------------------------------------------

class TocFieldLocation : public FieldLocation {
public:

    TocFieldLocation(const TocFieldLocation& rhs);
    TocFieldLocation(const eckit::PathName path, eckit::Offset offset, eckit::Length length, const Key& remapKey);
    TocFieldLocation(const eckit::URI& uri);
    TocFieldLocation(const eckit::URI& uri, eckit::Offset offset, eckit::Length length, const Key& remapKey);
    TocFieldLocation(const UriStore& store, const FieldRef& ref);
    TocFieldLocation(eckit::Stream&);

    eckit::DataHandle* dataHandle() const override;

    std::shared_ptr<const FieldLocation> make_shared() const override;

    void visit(FieldLocationVisitor& visitor) const override;

public:  // For Streamable

    static const eckit::ClassSpec& classSpec() { return classSpec_; }

protected:  // For Streamable

    const eckit::ReanimatorBase& reanimator() const override { return reanimator_; }
    void encode(eckit::Stream&) const override;

    static eckit::ClassSpec classSpec_;
    static eckit::Reanimator<TocFieldLocation> reanimator_;

private:  // methods

    void print(std::ostream& out) const override;
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5

#endif  // fdb5_TocFieldLocation_H

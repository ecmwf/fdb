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
#include "fdb5/database/FileStore.h"
#include "fdb5/toc/FieldRef.h"

namespace fdb5 {


//----------------------------------------------------------------------------------------------------------------------

class TocFieldLocation : public FieldLocation {
public:

    TocFieldLocation();
    TocFieldLocation(const TocFieldLocation& rhs);
    TocFieldLocation(const eckit::PathName &path, eckit::Offset offset, eckit::Length length);
    TocFieldLocation(const FileStore& store, const FieldRef& ref);

    const eckit::PathName& path() const { return path_; }
    const eckit::Offset&   offset() const { return offset_; }

    virtual eckit::DataHandle *dataHandle() const;

    virtual eckit::PathName url() const;

    virtual std::shared_ptr<FieldLocation> make_shared() const;

    virtual void visit(FieldLocationVisitor& visitor) const;

private: // methods

    virtual void dump(std::ostream &out) const;

    virtual void print(std::ostream &out) const;

private: // members

    eckit::PathName path_;
    eckit::Offset offset_;
};


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb5_TocFieldLocation_H

/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date Sep 2012

#ifndef fdb5_Field_H
#define fdb5_Field_H

#include "eckit/eckit.h"

#include "eckit/filesystem/PathName.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/types/Types.h"
#include "eckit/types/FixedString.h"

#include "fdb5/database/FileStore.h"
#include "fdb5/database/IndexAxis.h"
#include "fdb5/database/Key.h"
#include "fdb5/database/FieldLocation.h"
#include "fdb5/database/FieldDetails.h"

namespace eckit {
class DataHandle;
}

namespace fdb5 {

class FieldRef;
class FileStore;

class Field {
  public:
    // Field(const FileStore &);

    Field();
    Field(const FileStore &, const FieldRef &);
    Field(const eckit::PathName &path, eckit::Offset offset, eckit::Length length );


    eckit::DataHandle *dataHandle() const { return location_.dataHandle(); }

    const eckit::PathName &path() const { return location_.path(); }
    const eckit::Offset &offset() const { return location_.offset(); }
    const eckit::Length &length() const { return location_.length(); }

    const FieldLocation& location() const { return location_; }
    const FieldDetails& details() const { return details_; }

  private:
    FieldLocation location_;
    FieldDetails details_;

    void print( std::ostream &out ) const;

    friend std::ostream &operator<<(std::ostream &s, const Field &x) {
        x.print(s);
        return s;
    }

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
